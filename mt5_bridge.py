#!/usr/bin/env python3
"""
MT5 Bridge Server for XAU/USD Scalping System
Run this on your computer to connect to MT5
"""

import asyncio
import json
import logging
from datetime import datetime
from flask import Flask, jsonify, request
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import MetaTrader5 as mt5
import pandas as pd
import threading
import time
import os
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MT5Bridge:
    def __init__(self):
        self.app = Flask(__name__)
        CORS(self.app)
        self.socketio = SocketIO(self.app, cors_allowed_origins="*", async_mode='threading')
        self.mt5_connected = False
        self.account_info = None
        self.positions = []
        self.last_prices = {}
        
        # Setup routes
        self.setup_routes()
        self.setup_socket_events()
        
    def setup_routes(self):
        @self.app.route('/')
        def index():
            return jsonify({
                "status": "MT5 Bridge Server Running",
                "mt5_connected": self.mt5_connected,
                "endpoints": {
                    "/connect": "Connect to MT5",
                    "/account": "Get account info",
                    "/positions": "Get open positions",
                    "/symbols": "Get available symbols",
                    "/price/<symbol>": "Get symbol price"
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
                
                logger.info(f"Attempting to connect to MT5: Account={account}, Server={server}")
                
                authorized = mt5.login(
                    login=int(account),
                    password=password,
                    server=server
                )
                
                if authorized:
                    self.mt5_connected = True
                    self.account_info = mt5.account_info()._asdict()
                    
                    # Start price monitoring
                    threading.Thread(target=self.monitor_prices, daemon=True).start()
                    
                    logger.info(f"Connected to MT5 successfully. Account: {self.account_info['login']}")
                    
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
                        pos_dict['current_profit'] = profit
                    positions_data.append(pos_dict)
                
                self.positions = positions_data
                return jsonify(positions_data)
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/symbols', methods=['GET'])
        def get_symbols():
            """Get available symbols"""
            if not self.mt5_connected:
                return jsonify({"error": "Not connected to MT5"}), 400
            
            try:
                symbols = mt5.symbols_get()
                symbols_data = [s.name for s in symbols]
                return jsonify(symbols_data)
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/price/<symbol>', methods=['GET'])
        def get_price(symbol):
            """Get current price for symbol"""
            if not self.mt5_connected:
                return jsonify({"error": "Not connected to MT5"}), 400
            
            try:
                tick = mt5.symbol_info_tick(symbol)
                if tick:
                    return jsonify({
                        "symbol": symbol,
                        "bid": tick.bid,
                        "ask": tick.ask,
                        "last": tick.last,
                        "volume": tick.volume,
                        "time": str(tick.time)
                    })
                else:
                    return jsonify({"error": f"No price data for {symbol}"}), 404
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
                order_type = data.get('type', 'buy')  # 'buy' or 'sell'
                volume = float(data.get('volume', 0.01))
                stop_loss = float(data.get('stop_loss', 0))
                take_profit = float(data.get('take_profit', 0))
                comment = data.get('comment', 'Web App Trade')
                
                # Prepare the trade request
                symbol_info = mt5.symbol_info(symbol)
                if symbol_info is None:
                    return jsonify({"error": f"Symbol {symbol} not found"}), 404
                
                point = symbol_info.point
                
                if order_type == 'buy':
                    order_type_mt5 = mt5.ORDER_TYPE_BUY
                    price = mt5.symbol_info_tick(symbol).ask
                    if stop_loss > 0:
                        sl = price - stop_loss * point
                    else:
                        sl = 0
                    if take_profit > 0:
                        tp = price + take_profit * point
                    else:
                        tp = 0
                else:  # sell
                    order_type_mt5 = mt5.ORDER_TYPE_SELL
                    price = mt5.symbol_info_tick(symbol).bid
                    if stop_loss > 0:
                        sl = price + stop_loss * point
                    else:
                        sl = 0
                    if take_profit > 0:
                        tp = price - take_profit * point
                    else:
                        tp = 0
                
                # Prepare the request
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
                
                # Send the trade request
                result = mt5.order_send(request_dict)
                
                if result.retcode == mt5.TRADE_RETCODE_DONE:
                    logger.info(f"Trade executed: {order_type} {volume} {symbol} at {price}")
                    return jsonify({
                        "success": True,
                        "order_id": result.order,
                        "ticket": result.deal,
                        "volume": result.volume,
                        "price": result.price,
                        "bid": result.bid,
                        "ask": result.ask,
                        "comment": result.comment
                    })
                else:
                    logger.error(f"Trade failed: {result.comment}")
                    return jsonify({
                        "success": False,
                        "error": result.comment,
                        "retcode": result.retcode
                    }), 400
                    
            except Exception as e:
                logger.error(f"Trade execution error: {e}")
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
                    logger.info(f"Position {ticket} closed successfully")
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
        
        @self.app.route('/history', methods=['GET'])
        def get_history():
            """Get trading history"""
            if not self.mt5_connected:
                return jsonify({"error": "Not connected to MT5"}), 400
            
            try:
                # Get deals from last 24 hours
                from_date = datetime.now().timestamp() - 86400  # 24 hours ago
                deals = mt5.history_deals_get(from_date, datetime.now().timestamp())
                
                if deals:
                    deals_data = [deal._asdict() for deal in deals]
                    return jsonify(deals_data)
                else:
                    return jsonify([])
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        
        @self.app.route('/disconnect', methods=['POST'])
        def disconnect():
            """Disconnect from MT5"""
            try:
                mt5.shutdown()
                self.mt5_connected = False
                self.account_info = None
                self.positions = []
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
        
        @self.socketio.on('get_account')
        def handle_get_account():
            if self.mt5_connected:
                account = mt5.account_info()._asdict()
                emit('account_info', account)
        
        @self.socketio.on('get_positions')
        def handle_get_positions():
            if self.mt5_connected:
                positions = mt5.positions_get()
                positions_data = [pos._asdict() for pos in positions]
                emit('positions', positions_data)
        
        @self.socketio.on('get_price')
        def handle_get_price(data):
            symbol = data.get('symbol', 'XAUUSD')
            if self.mt5_connected:
                tick = mt5.symbol_info_tick(symbol)
                if tick:
                    emit('price_update', {
                        'symbol': symbol,
                        'bid': tick.bid,
                        'ask': tick.ask,
                        'time': str(tick.time)
                    })
        
        @self.socketio.on('place_order')
        def handle_place_order(data):
            # This would handle real-time order placement via WebSocket
            logger.info(f"WebSocket order request: {data}")
            # In production, you would implement this
    
    def monitor_prices(self):
        """Background thread to monitor prices and emit updates"""
        monitored_symbols = ['XAUUSD', 'EURUSD', 'GBPUSD', 'USDJPY']
        
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
                        self.socketio.emit('price_update', price_data)
                
                # Also update positions if they exist
                if self.mt5_connected:
                    positions = mt5.positions_get()
                    if positions:
                        positions_data = [pos._asdict() for pos in positions]
                        self.socketio.emit('positions_update', positions_data)
                
                time.sleep(1)  # Update every second
                
            except Exception as e:
                logger.error(f"Price monitoring error: {e}")
                time.sleep(5)
    
    def run(self, host='0.0.0.0', port=5000):
        """Run the bridge server"""
        logger.info(f"Starting MT5 Bridge Server on {host}:{port}")
        logger.info("Make sure MT5 is installed and running on this computer")
        logger.info("Access the web interface at http://localhost:5000")
        
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
