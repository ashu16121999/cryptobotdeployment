from binance import AsyncClient
import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import time  # Import the time module for accurate time synchronization

# Load environment variables
load_dotenv()

# Configure logging with milliseconds in the timestamp
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s,%(msecs)03d - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('bot_debug.log'),
        logging.StreamHandler()
    ]
)

IST = timezone(timedelta(hours=5, minutes=30))  # Indian Standard Time


class PrecisionFuturesTrader:
    def __init__(self):
        self.SYMBOL = 'AERGOUSDT'  # Trading pair
        self.FIXED_QTY = 850  # Quantity
        self.LEVERAGE = 6  # Leverage
        self.ENTRY_TIME = (13, 29, 59, 250)  # 01:29:59.250 PM IST
        self.time_offset = 0.0
        self.order_plan = []  # Store the sell order plan
        self.entry_time = None  # To store the entry timestamp

    async def _verify_connection(self, async_client):
        """Verify API connectivity"""
        try:
            account_info = await async_client.futures_account()
            if not account_info['canTrade']:
                raise PermissionError("Futures trading disabled")
            logging.info("API connection verified")
        except Exception as e:
            logging.error(f"Connection failed: {e}")
            raise

    async def _calibrate_time_sync(self, async_client):
        """Time synchronization with Binance server time using absolute timestamps."""
        measurements = []

        for _ in range(20):  # Perform multiple measurements for accuracy
            try:
                t0 = time.time() * 1000  # Current system time in milliseconds
                server_time = (await async_client.futures_time())['serverTime']  # Binance server time (ms)
                t1 = time.time() * 1000  # Current system time in milliseconds
                latency = t1 - t0
                offset = server_time - ((t0 + t1) / 2)  # Use the midpoint of t0 and t1 for accuracy
                measurements.append((latency, offset))
            except Exception as e:
                logging.warning(f"Time sync failed: {e}")

        avg_latency = sum(m[0] for m in measurements) / len(measurements) if measurements else 0
        self.time_offset = sum(m[1] for m in measurements) / len(measurements) if measurements else 0

        logging.info(f"Time synced | Offset: {self.time_offset:.2f}ms | Latency: {avg_latency:.2f}ms")

    async def _set_leverage(self, async_client):
        """Set the leverage for the trading pair"""
        await async_client.futures_change_leverage(
            symbol=self.SYMBOL,
            leverage=self.LEVERAGE
        )

    def _get_server_time(self):
        """Get the synchronized server time"""
        return time.time() * 1000 + self.time_offset  # Convert to milliseconds

    def _calculate_target(self, hour, minute, second, millisecond):
        """Calculate the target timestamp in Binance server time"""
        now = datetime.fromtimestamp(self._get_server_time() / 1000, IST)
        target = now.replace(
            hour=hour,
            minute=minute,
            second=second,
            microsecond=millisecond * 1000
        )
        return target.timestamp() * 1000  # Convert back to milliseconds

    async def _precision_wait(self, target_ts):
        """Enhanced wait with high precision"""
        while True:
            current = self._get_server_time()
            if current >= target_ts:
                return
            remaining = target_ts - current
            await asyncio.sleep(max(remaining / 2000, 0.001))  # Sleep for half the remaining time or 1ms

    async def _execute_order(self, async_client, side, quantity=None):
        """Execute a market order"""
        try:
            qty = quantity if quantity else self.FIXED_QTY
            order = await async_client.futures_create_order(
                symbol=self.SYMBOL,
                side=side,
                type='MARKET',  # Ensuring only market orders are used
                quantity=qty,
                newOrderRespType='FULL'
            )
            logging.info(f"Market {side} order executed successfully for {qty} contracts.")
            return order
        except Exception as e:
            logging.error(f"Failed to execute market {side} order: {e}")
            raise

    async def _fetch_order_book(self, async_client):
        """Fetch order book and create a sell plan"""
        try:
            order_book = await async_client.futures_order_book(symbol=self.SYMBOL, limit=50)
            bids = order_book['bids']  # Top 50 bids
            remaining_qty = self.FIXED_QTY
            self.order_plan = []

            for price, qty in bids:
                qty = float(qty)
                if remaining_qty <= 0:
                    break
                chunk_qty = min(remaining_qty, qty)
                self.order_plan.append((float(price), chunk_qty))
                remaining_qty -= chunk_qty

            logging.info(f"Sell order plan created: {self.order_plan}")
        except Exception as e:
            logging.error(f"Failed to fetch order book: {e}")
            raise

    async def _execute_sell_plan(self, async_client):
        """Execute the sell plan"""
        try:
            tasks = [
                self._execute_order(async_client, 'SELL', quantity=qty)
                for price, qty in self.order_plan
            ]
            await asyncio.gather(*tasks)  # Execute all sell orders in parallel
            logging.info("Sell orders executed successfully.")
        except Exception as e:
            logging.error(f"Failed to execute sell plan: {e}")
            raise

    async def _monitor_funding_fee_and_sell(self, async_client):
        """Monitor funding fee and execute the sell plan immediately after verification"""
        try:
            while True:
                income_history = await async_client.futures_income_history(
                    symbol=self.SYMBOL,
                    incomeType='FUNDING_FEE',
                    limit=10
                )
                for income in income_history:
                    funding_time = income['time']
                    if funding_time > self.entry_time:  # Ensure it's for the current trade
                        logging.info("\n=== FUNDING FEE DETECTED ===")
                        logging.info(f"Funding fee credited: {income}")
                        await self._execute_sell_plan(async_client)  # Execute the sell plan immediately
                        return
                await asyncio.sleep(0.01)  # Check every 10ms
        except Exception as e:
            logging.error(f"Error while monitoring funding fee: {e}")
            raise

    async def execute_strategy(self):
        """Main strategy execution"""
        async_client = await AsyncClient.create(
            os.getenv('API_KEY'), os.getenv('API_SECRET')
        )
        try:
            # Verify connection and setup
            await self._verify_connection(async_client)
            await self._set_leverage(async_client)
            await self._calibrate_time_sync(async_client)

            # Calculate entry target and wait until precise time
            entry_target = self._calculate_target(*self.ENTRY_TIME)
            await self._precision_wait(entry_target)

            logging.info("\n=== ENTRY TRIGGERED ===")
            # Execute market buy order
            buy_order = await self._execute_order(async_client, 'BUY')
            self.entry_time = self._get_server_time()  # Capture entry time for funding fee monitoring

            # Create sell plan immediately after buying
            await self._fetch_order_book(async_client)

            # Monitor funding fees and sell in parallel
            await self._monitor_funding_fee_and_sell(async_client)

        except Exception as e:
            logging.error(f"Strategy failed: {e}")
        finally:
            await async_client.close_connection()


if __name__ == "__main__":
    trader = PrecisionFuturesTrader()
    asyncio.run(trader.execute_strategy())