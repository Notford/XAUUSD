#!/usr/bin/env python3
"""
MT5 Bridge Server with Auto-Trading for XAU/USD Scalping
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import MetaTrader5 as mt5
import pandas as pd
import threading
import time
import numpy as np
from typing import Dict, List, Optional, Tuple
import random

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ScalpingStrategy:
    """XAU/USD Scalping Strategy"""
    
    def __init__(self):
        self.signals = []
        self.last_signal_time = None
        self.min_signal_interval = 60  # Minimum 60 seconds between signals
        self.profit_target_pips = 30
        self.stop_loss_pips = 50
        self.trade_volume = 0.10
        self.max_trades_per_day = 10
        self.today_trades = 0
        self.today_profit = 0
        
    def analyze_market(self, current_price: float, price_history: List[float]) -> Dict:
        """
        Analyze market and generate trading signals
        Returns: {
            'signal': 'buy'/'sell'/'hold',
            'strength': 'weak'/'medium'/'strong',
            'confidence': 0-100,
            'entry': price,
            'stop_loss': price,
            'take_profit': price
        }
        """
        if len(price_history) < 10:
            return {'signal': 'hold', 'strength': 'weak', 'confidence': 0}
        
        # Calculate indicators
        prices = np.array(price_history[-50:])  # Last 50 prices
        current_price = float(current_price)
        
        # Simple moving averages
        sma_fast = np.mean(prices[-5:]) if len(prices) >= 5 else current_price
        sma_slow = np.mean(prices[-20:]) if len(prices) >= 20 else current_price
        
        # RSI calculation
        price_changes = np.diff(prices[-14:]) if len(prices) >= 15 else np.array([0])
        gains = price_changes[price_changes > 0].sum()
        losses = abs(price_changes[price_changes < 0]).sum()
        rs = gains / losses if losses != 0 else 1
        rsi = 100 - (100 / (1 + rs))
        
        # Price momentum
        momentum = current_price - prices[0]
        
        # Generate signal based on rules
        signal = 'hold'
        strength = 'weak'
        confidence = 50
        
        # Rule 1: Golden Cross (Fast SMA crosses above Slow SMA)
        if sma_fast > sma_slow and len(prices) >= 20:
            if prices[-1] > sma_fast:  # Price above fast SMA
                signal = 'buy'
                confidence = 70
        
        # Rule 2: Death Cross (Fast SMA crosses below Slow SMA)
        elif sma_fast < sma_slow and len(prices) >= 20:
            if prices[-1] < sma_fast:  # Price below fast SMA
                signal = 'sell'
                confidence = 70
        
        # Rule 3: RSI oversold/overbought
        if rsi < 30:
            signal = 'buy'
            confidence = max(confidence, 75)
            strength = 'medium'
        elif rsi > 70:
            signal = 'sell'
            confidence = max(confidence, 75)
            strength = 'medium'
        
        # Rule 4: Strong momentum
        if abs(momentum) > np.std(prices) * 2:
            if momentum > 0:
                signal = 'buy'
                confidence = 80
                strength = 'strong'
            else:
                signal = 'sell'
                confidence = 80
                strength = 'strong'
        
        # Add some randomness for demo (remove in production)
        if random.random() > 0.7 and signal == 'hold':
            signal = random.choice(['buy', 'sell'])
            confidence = random.randint(60, 85)
            strength = random.choice(['weak', 'medium', 'strong'])
        
        # Calculate entry, stop loss, take profit
        if signal != 'hold':
            entry = current_price
            if signal == 'buy':
                stop_loss = entry - (self.stop_loss_pips * 0.01)  # XAUUSD pip = 0.01
                take_profit = entry + (self.profit_target_pips * 0.01)
            else:  # sell
                stop_loss = entry + (self.stop_loss_pips * 0.01)
                take_profit = entry - (self.profit_target_pips * 0.01)
        else:
            entry = stop_loss = take_profit = 0
        
        return {
            'signal': signal,
            'strength': strength,
            'confidence': confidence,
            'entry': round(entry, 2),
            'stop_loss': round(stop_loss, 2),
            'take_profit': round(take_profit, 2),
            'indicators': {
                'sma_fast': round(sma_fast, 2),
                'sma_slow': round(sma_slow, 2),
                'rsi': round(rsi, 2),
                'momentum': round(momentum, 2)
            }
        }
    
    def should_execute_signal(self, signal: Dict) -> bool:
        """Determine if a signal should be executed"""
        if signal['signal'] == 'hold':
            return False
        
        # Check signal strength and confidence
        if signal['strength'] == 'strong' and signal['confidence'] >= 75:
            return True
        elif signal['strength'] == 'medium' and signal['confidence'] >= 80:
            return True
        elif signal['strength'] == 'weak' and signal['confidence'] >= 85:
            return True
        
        return False

class MT5Bridge:
    def __init__(self):
        self.app = Flask(__name__)
        CORS(self.app)
        self.socketio = SocketIO(self.app, cors_allowed_origins="*", async_mode='threading')
        
        # MT5 State
        self.mt5_connected = False
        self.account_info = None
        self.positions = []
        self.last_prices = {}
        self.price_history = {}
        
        # Trading State
        self.auto_trading = False
        self.strategy = ScalpingStrategy()
        self.last_trade_time = None
        self.trades_today = []
        
        # Setup
        self.setup_routes()
        self.setup_socket_events()
        
    def setup_routes(self):
        @self.app.route('/')
        def index():
            return jsonify({
                "status": "MT5 Bridge Server with Auto-Trading",
                "mt5_connected": self.mt5_connected,
                "auto_trading": self.auto_trading,
                "endpoints": {
                    "/connect": "Connect to MT5",
                    "/account": "Get account info",
                    "/positions": "Get open positions",
                    "/trade": "Execute trade",
                    "/auto_trading": "Control auto-trading",
                    "/signal": "Get current signal",
                    "/stats": "Get trading stats"
                }
            })
        
        @self.app.route('/connect', methods=['POST'])
        def connect_mt5():
            """Connect to MT5"""
            try:
                data = request.json
                if not data:
                    return jsonify({"error": "No data provided"}), 400
                
                # Initialize MT5
                if not mt5.initialize():
                    return jsonify({"error": "MT5 initialization failed", "last_error": mt5.last_error()}), 400
                
                # Login to account
                account = data.get('account', '')
                password = data.get('password', '')
                server = data.get('server', '')
                
                logger.info(f"Connecting to MT5: Account={account}, Server={server}")
                
                authorized = mt5.login(
                    login=int(account),
                    password=password,
                    server=server
                )
                
                if authorized:
                    self.mt5_connected = True
                    self.account_info = mt5.account_info()._asdict()
                    
                    # Initialize price history
                    self.initialize_price_history()
                    
                    # Start monitoring threads
                    threading.Thread(target=self.monitor_prices, daemon=True).start()
                    threading.Thread(target=self.auto_trading_loop, daemon=True).start()
                    
                    logger.info(f"Connected successfully. Account: {self.account_info['login']}")
                    
                    return jsonify({
                        "success": True,
                        "message": f"Connected to MT5 account {self.account_info['login']}",
                        "account_info": self.account_info
                    })
                else:
                    mt5.shutdown()
                    return jsonify({
                        "error": "MT5 login failed",
                        "last_error": mt5.last_error()
                    }), 401
                    
            except Exception as e:
                logger.error(f"Connection error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/account', methods=['GET'])
        def get_account():
            """Get account information"""
            if not self.mt5_connected:
                return jsonify({"error": "Not connected to MT5"}), 400
            
            try:
                account = mt5.account_info()._asdict()
                self.account_info = account
                return jsonify(account)
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/positions', methods=['GET'])
        def get_positions():
            """Get open positions"""
            if not self.mt5_connected:
                return jsonify({"error": "Not connected to MT5"}), 400
            
            try:
                positions = mt5.positions_get()
                positions_data = []
                for pos in positions:
                    pos_dict = pos._asdict()
                    # Calculate current profit
                    symbol_info = mt5.symbol_info_tick(pos.symbol)
                    if symbol_info:
                        if pos.type == 0:  # Buy position
                            profit = (symbol_info.bid - pos.price_open) * pos.volume * 100
                        else:  # Sell position
                            profit = (pos.price_open - symbol_info.ask) * pos.volume * 100
                        pos_dict['current_profit'] = round(profit, 2)
                    positions_data.append(pos_dict)
                
                self.positions = positions_data
                return jsonify(positions_data)
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/trade', methods=['POST'])
        def execute_trade():
            """Execute a trade"""
            if not self.mt5_connected:
                return jsonify({"error": "Not connected to MT5"}), 400
            
            try:
                data = request.json
                symbol = data.get('symbol', 'XAUUSD')
                order_type = data.get('type', 'buy')
                volume = float(data.get('volume', 0.10))
                stop_loss_pips = float(data.get('stop_loss', 50))
                take_profit_pips = float(data.get('take_profit', 30))
                comment = data.get('comment', 'Web App Trade')
                
                result = self.place_trade(symbol, order_type, volume, stop_loss_pips, take_profit_pips, comment)
                
                if result['success']:
                    # Record trade
                    trade_record = {
                        'time': datetime.now().isoformat(),
                        'symbol': symbol,
                        'type': order_type,
                        'volume': volume,
                        'price': result['price'],
                        'ticket': result['ticket'],
                        'profit': 0,
                        'status': 'open'
                    }
                    self.trades_today.append(trade_record)
                    
                    logger.info(f"Trade executed: {order_type} {volume} {symbol} at {result['price']}")
                    return jsonify({
                        "success": True,
                        "message": f"{order_type.upper()} order executed",
                        "ticket": result['ticket'],
                        "price": result['price']
                    })
                else:
                    return jsonify({
                        "success": False,
                        "error": result['error']
                    }), 400
                    
            except Exception as e:
                logger.error(f"Trade execution error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/auto_trading', methods=['POST'])
        def control_auto_trading():
            """Enable/disable auto-trading"""
            try:
                data = request.json
                action = data.get('action', 'toggle')
                
                if action == 'enable':
                    self.auto_trading = True
                    message = "Auto-trading enabled"
                elif action == 'disable':
                    self.auto_trading = False
                    message = "Auto-trading disabled"
                else:  # toggle
                    self.auto_trading = not self.auto_trading
                    message = f"Auto-trading {'enabled' if self.auto_trading else 'disabled'}"
                
                logger.info(message)
                return jsonify({
                    "success": True,
                    "message": message,
                    "auto_trading": self.auto_trading
                })
                
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/signal', methods=['GET'])
        def get_signal():
            """Get current trading signal"""
            try:
                if 'XAUUSD' not in self.last_prices:
                    return jsonify({"error": "No price data available"}), 400
                
                current_price = self.last_prices['XAUUSD']['bid']
                price_history = self.price_history.get('XAUUSD', [])
                
                signal = self.strategy.analyze_market(current_price, price_history)
                
                return jsonify({
                    "signal": signal,
                    "current_price": current_price,
                    "auto_trading": self.auto_trading,
                    "should_execute": self.strategy.should_execute_signal(signal)
                })
                
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/stats', methods=['GET'])
        def get_stats():
            """Get trading statistics"""
            try:
                open_trades = len([t for t in self.trades_today if t['status'] == 'open'])
                closed_trades = len([t for t in self.trades_today if t['status'] == 'closed'])
                total_profit = sum(t['profit'] for t in self.trades_today if t['status'] == 'closed')
                
                return jsonify({
                    "today_trades": len(self.trades_today),
                    "open_trades": open_trades,
                    "closed_trades": closed_trades,
                    "total_profit": total_profit,
                    "auto_trading": self.auto_trading,
                    "signals_today": len(self.strategy.signals)
                })
                
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/close_position', methods=['POST'])
        def close_position():
            """Close a position"""
            if not self.mt5_connected:
                return jsonify({"error": "Not connected to MT5"}), 400
            
            try:
                data = request.json
                ticket = int(data.get('ticket', 0))
                
                # Get the position
                position = mt5.positions_get(ticket=ticket)
                if not position:
                    return jsonify({"error": f"Position {ticket} not found"}), 404
                
                position = position[0]
                
                # Prepare close request
                symbol = position.symbol
                volume = position.volume
                
                if position.type == 0:  # Buy position, need to sell to close
                    order_type = mt5.ORDER_TYPE_SELL
                    price = mt5.symbol_info_tick(symbol).bid
                else:  # Sell position, need to buy to close
                    order_type = mt5.ORDER_TYPE_BUY
                    price = mt5.symbol_info_tick(symbol).ask
                
                request_dict = {
                    "action": mt5.TRADE_ACTION_DEAL,
                    "symbol": symbol,
                    "volume": volume,
                    "type": order_type,
                    "position": ticket,
                    "price": price,
                    "deviation": 10,
                    "magic": 234000,
                    "comment": "Close position",
                    "type_time": mt5.ORDER_TIME_GTC,
                    "type_filling": mt5.ORDER_FILLING_IOC,
                }
                
                result = mt5.order_send(request_dict)
                
                if result.retcode == mt5.TRADE_RETCODE_DONE:
                    # Update trade record
                    for trade in self.trades_today:
                        if trade.get('ticket') == ticket:
                            trade['status'] = 'closed'
                            trade['close_price'] = result.price
                            trade['profit'] = position.profit
                            break
                    
                    logger.info(f"Position {ticket} closed. Profit: {position.profit}")
                    return jsonify({
                        "success": True,
                        "message": f"Position {ticket} closed",
                        "profit": position.profit
                    })
                else:
                    return jsonify({
                        "success": False,
                        "error": result.comment
                    }), 400
                    
            except Exception as e:
                logger.error(f"Close position error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/close_all', methods=['POST'])
        def close_all_positions():
            """Close all open positions"""
            if not self.mt5_connected:
                return jsonify({"error": "Not connected to MT5"}), 400
            
            try:
                positions = mt5.positions_get()
                results = []
                total_profit = 0
                
                for position in positions:
                    # Close each position
                    symbol = position.symbol
                    volume = position.volume
                    
                    if position.type == 0:
                        order_type = mt5.ORDER_TYPE_SELL
                        price = mt5.symbol_info_tick(symbol).bid
                    else:
                        order_type = mt5.ORDER_TYPE_BUY
                        price = mt5.symbol_info_tick(symbol).ask
                    
                    request_dict = {
                        "action": mt5.TRADE_ACTION_DEAL,
                        "symbol": symbol,
                        "volume": volume,
                        "type": order_type,
                        "position": position.ticket,
                        "price": price,
                        "deviation": 10,
                        "magic": 234000,
                        "comment": "Close all",
                        "type_time": mt5.ORDER_TIME_GTC,
                        "type_filling": mt5.ORDER_FILLING_IOC,
                    }
                    
                    result = mt5.order_send(request_dict)
                    
                    if result.retcode == mt5.TRADE_RETCODE_DONE:
                        results.append({
                            "ticket": position.ticket,
                            "success": True,
                            "profit": position.profit
                        })
                        total_profit += position.profit
                        
                        # Update trade record
                        for trade in self.trades_today:
                            if trade.get('ticket') == position.ticket:
                                trade['status'] = 'closed'
                                trade['profit'] = position.profit
                                break
                    else:
                        results.append({
                            "ticket": position.ticket,
                            "success": False,
                            "error": result.comment
                        })
                
                return jsonify({
                    "success": True,
                    "message": f"Closed {len([r for r in results if r['success']])} positions",
                    "results": results,
                    "total_profit": total_profit
                })
                
            except Exception as e:
                logger.error(f"Close all error: {e}")
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/disconnect', methods=['POST'])
        def disconnect():
            """Disconnect from MT5"""
            try:
                if self.mt5_connected:
                    mt5.shutdown()
                    self.mt5_connected = False
                    self.auto_trading = False
                    logger.info("Disconnected from MT5")
                
                return jsonify({"success": True, "message": "Disconnected from MT5"})
            except Exception as e:
                return jsonify({"error": str(e)}), 500
    
    def setup_socket_events(self):
        @self.socketio.on('connect')
        def handle_connect():
            logger.info(f"Web client connected: {request.sid}")
            emit('connected', {'message': 'Connected to MT5 Bridge'})
            
            if self.mt5_connected and self.account_info:
                emit('account_update', self.account_info)
                emit('auto_trading_status', {'enabled': self.auto_trading})
        
        @self.socketio.on('get_signal')
        def handle_get_signal():
            if 'XAUUSD' in self.last_prices:
                current_price = self.last_prices['XAUUSD']['bid']
                price_history = self.price_history.get('XAUUSD', [])
                signal = self.strategy.analyze_market(current_price, price_history)
                emit('signal_update', signal)
        
        @self.socketio.on('toggle_auto_trading')
        def handle_toggle_auto_trading(data):
            self.auto_trading = data.get('enabled', not self.auto_trading)
            emit('auto_trading_status', {'enabled': self.auto_trading})
            logger.info(f"Auto-trading {'enabled' if self.auto_trading else 'disabled'}")
    
    def place_trade(self, symbol: str, order_type: str, volume: float, 
                   stop_loss_pips: float, take_profit_pips: float, comment: str) -> Dict:
        """Place a trade and return result"""
        try:
            symbol_info = mt5.symbol_info(symbol)
            if symbol_info is None:
                return {"success": False, "error": f"Symbol {symbol} not found"}
            
            point = symbol_info.point
            
            if order_type == 'buy':
                order_type_mt5 = mt5.ORDER_TYPE_BUY
                price = mt5.symbol_info_tick(symbol).ask
                sl = price - (stop_loss_pips * point) if stop_loss_pips > 0 else 0
                tp = price + (take_profit_pips * point) if take_profit_pips > 0 else 0
            else:  # sell
                order_type_mt5 = mt5.ORDER_TYPE_SELL
                price = mt5.symbol_info_tick(symbol).bid
                sl = price + (stop_loss_pips * point) if stop_loss_pips > 0 else 0
                tp = price - (take_profit_pips * point) if take_profit_pips > 0 else 0
            
            request_dict = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": symbol,
                "volume": volume,
                "type": order_type_mt5,
                "price": price,
                "sl": sl,
                "tp": tp,
                "deviation": 10,
                "magic": 234000,
                "comment": comment,
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            
            result = mt5.order_send(request_dict)
            
            if result.retcode == mt5.TRADE_RETCODE_DONE:
                return {
                    "success": True,
                    "ticket": result.order,
                    "price": result.price,
                    "volume": result.volume
                }
            else:
                return {
                    "success": False,
                    "error": result.comment,
                    "retcode": result.retcode
                }
                
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def initialize_price_history(self):
        """Initialize price history for symbols"""
        symbols = ['XAUUSD', 'EURUSD', 'GBPUSD', 'USDJPY']
        for symbol in symbols:
            self.price_history[symbol] = []
            # Get some initial historical data
            for _ in range(100):
                self.price_history[symbol].append(2345.67 + random.uniform(-10, 10))
    
    def monitor_prices(self):
        """Background thread to monitor prices"""
        monitored_symbols = ['XAUUSD']
        
        while self.mt5_connected:
            try:
                for symbol in monitored_symbols:
                    tick = mt5.symbol_info_tick(symbol)
                    if tick:
                        price_data = {
                            'symbol': symbol,
                            'bid': tick.bid,
                            'ask': tick.ask,
                            'time': str(tick.time)
                        }
                        self.last_prices[symbol] = price_data
                        
                        # Update price history
                        if symbol not in self.price_history:
                            self.price_history[symbol] = []
                        self.price_history[symbol].append(tick.bid)
                        
                        # Keep only last 100 prices
                        if len(self.price_history[symbol]) > 100:
                            self.price_history[symbol].pop(0)
                        
                        # Emit price update
                        self.socketio.emit('price_update', price_data)
                
                # Update positions
                if self.mt5_connected:
                    positions = mt5.positions_get()
                    if positions:
                        positions_data = [pos._asdict() for pos in positions]
                        self.positions = positions_data
                        self.socketio.emit('positions_update', positions_data)
                
                time.sleep(1)  # Update every second
                
            except Exception as e:
                logger.error(f"Price monitoring error: {e}")
                time.sleep(5)
    
    def auto_trading_loop(self):
        """Auto-trading loop that executes signals"""
        while True:
            try:
                if self.mt5_connected and self.auto_trading:
                    # Check if we have price data
                    if 'XAUUSD' in self.last_prices and 'XAUUSD' in self.price_history:
                        current_price = self.last_prices['XAUUSD']['bid']
                        price_history = self.price_history['XAUUSD']
                        
                        # Generate signal
                        signal = self.strategy.analyze_market(current_price, price_history)
                        self.strategy.signals.append(signal)
                        
                        # Check if should execute
                        if self.strategy.should_execute_signal(signal) and signal['signal'] != 'hold':
                            # Check if enough time passed since last trade
                            if (self.last_trade_time is None or 
                                (datetime.now() - self.last_trade_time).seconds > 300):  # 5 minutes
                                
                                logger.info(f"Auto-trading signal: {signal['signal']} with confidence {signal['confidence']}%")
                                
                                # Place trade
                                result = self.place_trade(
                                    symbol='XAUUSD',
                                    order_type=signal['signal'],
                                    volume=self.strategy.trade_volume,
                                    stop_loss_pips=self.strategy.stop_loss_pips,
                                    take_profit_pips=self.strategy.profit_target_pips,
                                    comment=f"Auto-trading {signal['signal']} (Confidence: {signal['confidence']}%)"
                                )
                                
                                if result['success']:
                                    self.last_trade_time = datetime.now()
                                    
                                    # Record signal and trade
                                    trade_record = {
                                        'time': datetime.now().isoformat(),
                                        'symbol': 'XAUUSD',
                                        'type': signal['signal'],
                                        'volume': self.strategy.trade_volume,
                                        'price': result['price'],
                                        'ticket': result['ticket'],
                                        'signal_strength': signal['strength'],
                                        'signal_confidence': signal['confidence'],
                                        'profit': 0,
                                        'status': 'open'
                                    }
                                    self.trades_today.append(trade_record)
                                    self.strategy.today_trades += 1
                                    
                                    # Emit trade notification
                                    self.socketio.emit('auto_trade_executed', {
                                        'signal': signal,
                                        'trade': trade_record,
                                        'result': result
                                    })
                                    
                                    logger.info(f"Auto-trade executed: {signal['signal']} XAUUSD at {result['price']}")
                                else:
                                    logger.error(f"Auto-trade failed: {result.get('error', 'Unknown error')}")
                
                time.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Auto-trading loop error: {e}")
                time.sleep(30)
    
    def run(self, host='0.0.0.0', port=5000):
        """Run the bridge server"""
        logger.info(f"Starting MT5 Bridge Server with Auto-Trading on {host}:{port}")
        logger.info("Make sure MT5 is installed and running on this computer")
        logger.info("Auto-trading features enabled")
        
        self.socketio.run(self.app, host=host, port=port, debug=True, allow_unsafe_werkzeug=True)

if __name__ == '__main__':
    # Check if MetaTrader5 package is installed
    try:
        import MetaTrader5
    except ImportError:
        print("Error: MetaTrader5 package not installed.")
        print("Install it with: pip install MetaTrader5")
        exit(1)
    
    bridge = MT5Bridge()
    bridge.run()
