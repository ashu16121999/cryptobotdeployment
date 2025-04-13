from binance import AsyncClient
import os
import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger()

IST = timezone(timedelta(hours=5, minutes=30))

class FundingFeeOptimizer:
    def __init__(self):
        self.SYMBOL = 'VIDTUSDT'  # Trading pair
        self.QTY = 50000  # Quantity in coins
        self.LEVERAGE = 7  # Leverage
        self.ENTRY_TARGET = self._calc_target(23, 29, 59, 150)  # Entry time (11:29:59:150 PM IST)
        self.EXIT_TARGET = self._calc_target(23, 30, 0, 50)  # Exit time (11:30:00:050 PM IST)
        self.time_offset = 0.0
        self.orderbook = None
        self.price_precision = None
        self.quantity_precision = None
        self.min_price = None
        self.min_qty = None
        logging.info(f"Bot initialized with ENTRY_TARGET: {self.ENTRY_TARGET}, EXIT_TARGET: {self.EXIT_TARGET}")

    def _calc_target(self, hour, minute, second, millis):
        """Calculate target timestamp with IST conversion"""
        now = datetime.now(IST)
        target = now.replace(
            hour=hour, minute=minute, second=second,
            microsecond=millis * 1000
        )
        return target.timestamp() + (1 if target < now else 0)

    async def _sync_time(self, client):
        """Sync time with Binance server using REST API"""
        try:
            server_time = (await client.futures_time())['serverTime'] / 1000
            self.time_offset = server_time - time.perf_counter()
            logging.info(f"Time synchronized with Binance server. Offset: {self.time_offset}s")
        except Exception as e:
            logging.error(f"Failed to sync time via REST API: {e}")
            raise

    async def _get_precision(self, client, symbol):
        """Fetch precision rules for the trading pair"""
        exchange_info = await client.futures_exchange_info()
        for symbol_info in exchange_info['symbols']:
            if symbol_info['symbol'] == symbol:
                self.price_precision = int(symbol_info['pricePrecision'])
                self.quantity_precision = int(symbol_info['quantityPrecision'])
                self.min_price = float(symbol_info['filters'][0]['minPrice'])
                self.min_qty = float(symbol_info['filters'][1]['minQty'])
                logging.info(f"Fetched precision rules for {symbol}: price_precision={self.price_precision}, quantity_precision={self.quantity_precision}, min_price={self.min_price}, min_qty={self.min_qty}")
                return
        raise ValueError(f"Symbol {symbol} not found in exchange info")

    def _round_to_precision(self, value, precision):
        """Round a value to the specified number of decimal places"""
        return round(value, precision)

    def _validate_price_and_quantity(self, price, qty):
        """Validate price and quantity against Binance's minimum requirements"""
        if price < self.min_price:
            raise ValueError(f"Price {price} is below the minimum allowed price {self.min_price}")
        if qty < self.min_qty:
            raise ValueError(f"Quantity {qty} is below the minimum allowed quantity {self.min_qty}")

    async def _get_orderbook(self, client):
        """Fetch optimized order book snapshot"""
        self.orderbook = await client.futures_order_book(
            symbol=self.SYMBOL,
            limit=5
        )
        logging.info(f"Order book fetched for {self.SYMBOL}")

    def _get_limit_price(self, side):
        """Calculate aggressive limit price from order book, rounded to precision"""
        if side == 'BUY':
            price = float(self.orderbook['asks'][1][0]) * 1.0001  # Above 2nd ask
        else:
            price = float(self.orderbook['bids'][1][0]) * 0.9999  # Below 2nd bid
        # Round price to allowed precision
        return self._round_to_precision(price, self.price_precision)

    async def _execute_market_order(self, client, side):
        """Execute a market order"""
        try:
            await client.futures_create_order(
                symbol=self.SYMBOL,
                side=side,
                type='MARKET',
                quantity=self.QTY
            )
            logging.info(f"Market {side} order executed successfully.")
        except Exception as e:
            logging.error(f"Failed to execute market {side} order: {e}")
            raise

    async def _hybrid_execute(self, client, side):
        """Limit + Market fallback execution with guaranteed position closure"""
        try:
            # Phase 1: Try limit order
            limit_price = self._get_limit_price(side)
            logging.info(f"Calculated limit price for {side}: {limit_price}")
            self._validate_price_and_quantity(limit_price, self.QTY)
            order = await client.futures_create_order(
                symbol=self.SYMBOL,
                side=side,
                type='LIMIT',
                timeInForce='IOC',
                quantity=self.QTY,
                price=limit_price
            )
            
            # Check immediate fill
            if float(order['executedQty']) >= self.QTY * 0.95:
                logging.info(f"Limit {side} order filled successfully.")
                return True
            else:
                logging.warning(f"Limit {side} order not fully filled. Attempting market fallback...")
        
        except Exception as e:
            logging.error(f"Limit {side} order failed with error: {e}. Switching to market order...")

        # Phase 2: Market order fallback
        try:
            await self._execute_market_order(client, side)
            logging.info(f"Market {side} order executed after limit order failure.")
        except Exception as e:
            logging.error(f"Market {side} order execution failed: {e}")
            raise e  # Re-raise the exception if the fallback fails

    async def _precision_countdown(self, target):
        """Microsecond-precise wait with spinlock"""
        logging.info(f"Waiting for target time: {target}")
        while (time.perf_counter() + self.time_offset) < target - 0.005:
            await asyncio.sleep(0)
        while (time.perf_counter() + self.time_offset) < target:
            pass
        logging.info(f"Reached target time: {target}")

    async def execute_strategy(self):
        logging.info("Starting execution strategy...")
        client = await AsyncClient.create(
            os.getenv('API_KEY'),
            os.getenv('API_SECRET')
        )

        try:
            # Initial setup
            await self._sync_time(client)
            await self._get_precision(client, self.SYMBOL)
            await client.futures_change_leverage(
                symbol=self.SYMBOL,
                leverage=self.LEVERAGE
            )
            logging.info(f"Leverage set to {self.LEVERAGE}x for {self.SYMBOL}")

            # Ensure quantity is rounded to allowed precision
            self.QTY = self._round_to_precision(self.QTY, self.quantity_precision)
            self._validate_price_and_quantity(1.0, self.QTY)  # Example validation

            # Entry execution (Market Order)
            logging.info(f"Preparing to execute entry order at {self.ENTRY_TARGET}")
            await self._precision_countdown(self.ENTRY_TARGET - 0.15)
            await self._execute_market_order(client, 'BUY')

            # Exit execution (Hybrid: Limit + Market Fallback)
            logging.info(f"Preparing to execute exit order at {self.EXIT_TARGET}")
            await self._precision_countdown(self.EXIT_TARGET - 0.15)
            await self._get_orderbook(client)
            await self._hybrid_execute(client, 'SELL')

        finally:
            await client.close_connection()
            logging.info("Bot execution completed. Connection closed.")

if __name__ == "__main__":
    try:
        logging.info("Starting FundingFeeOptimizer bot...")
        asyncio.run(FundingFeeOptimizer().execute_strategy())
    except Exception as e:
        logging.error(f"Unhandled exception occurred: {e}")