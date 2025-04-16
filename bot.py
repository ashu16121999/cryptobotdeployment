from binance import AsyncClient
import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

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
        self.SYMBOL = 'BSWUSDT'  # Trading pair
        self.FIXED_QTY = 25000  # Quantity
        self.LEVERAGE = 20  # Leverage
        self.ENTRY_TIME = (21, 29, 59, 250)  # 09:29:59.250 PM IST
        self.time_offset = 0.0
        self.order_plan = []  # To store the sell order plan based on order book

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
        """Time synchronization with dynamic compensation"""
        measurements = []
        for _ in range(20):  # Perform multiple measurements for accuracy
            try:
                # Record local time before the API call
                t0 = asyncio.get_event_loop().time() * 1000  # Convert to milliseconds
                server_time = (await async_client.futures_time())['serverTime']  # Already in milliseconds
                # Record local time after the API call
                t1 = asyncio.get_event_loop().time() * 1000  # Convert to milliseconds
                # Calculate round-trip latency and offset
                latency = t1 - t0
                offset = server_time - ((t0 + t1) / 2)  # Use midpoint for accuracy
                measurements.append((latency, offset))
            except Exception as e:
                logging.warning(f"Time sync failed: {e}")

        # Calculate average latency and offset
        avg_latency = sum(m[0] for m in measurements) / len(measurements)
        self.time_offset = sum(m[1] for m in measurements) / len(measurements)
        logging.info(f"Time synced | Offset: {self.time_offset:.2f}ms | Latency: {avg_latency:.2f}ms")

    async def _set_leverage(self, async_client):
        await async_client.futures_change_leverage(
            symbol=self.SYMBOL,
            leverage=self.LEVERAGE
        )

    def _get_server_time(self):
        return asyncio.get_event_loop().time() * 1000 + self.time_offset  # Convert to milliseconds

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
            await async_client.futures_create_order(
                symbol=self.SYMBOL,
                side=side,
                type='MARKET',  # Ensuring only market orders are used
                quantity=qty,
                newOrderRespType='FULL'
            )
            logging.info(f"Market {side} order executed successfully for {qty} contracts.")
        except Exception as e:
            logging.error(f"Failed to execute market {side} order: {e}")
            raise

    async def _fetch_order_book(self, async_client):
        """Fetch the order book and create a sell order plan"""
        try:
            # Fetch the order book (bids only)
            order_book = await async_client.futures_order_book(symbol=self.SYMBOL, limit=50)
            bids = order_book['bids']  # List of [price, quantity] at each bid level

            remaining_qty = self.FIXED_QTY
            self.order_plan = []

            for price, qty in bids:
                qty = float(qty)  # Convert quantity to float
                if remaining_qty <= 0:
                    break
                chunk_qty = min(remaining_qty, qty)
                self.order_plan.append((float(price), chunk_qty))
                remaining_qty -= chunk_qty

            logging.info(f"Sell order plan created based on order book: {self.order_plan}")

        except Exception as e:
            logging.error(f"Failed to fetch order book: {e}")
            raise

    async def _execute_sell_plan(self, async_client):
        """Execute the sell order plan based on pre-fetched order book data"""
        try:
            # Create tasks for all sell orders
            tasks = [
                self._execute_order(async_client, 'SELL', quantity=qty)
                for price, qty in self.order_plan
            ]
            # Execute all sell orders in parallel
            await asyncio.gather(*tasks)
            logging.info("Sell orders executed successfully in parallel.")
        except Exception as e:
            logging.error(f"Failed to execute sell order plan: {e}")
            raise

    async def _wait_for_funding_fee(self, async_client, entry_time):
        """Wait for funding fee to be credited for the current trade."""
        try:
            latest_funding_time = entry_time  # Start checking from the trade entry time

            while True:
                # Fetch funding fee history for the trading pair
                income_history = await async_client.futures_income_history(
                    symbol=self.SYMBOL,
                    incomeType='FUNDING_FEE',
                    limit=10  # Fetch the latest funding fee records
                )

                if income_history:
                    for income in income_history:
                        funding_time = income['time']  # Already in milliseconds
                        # Check if funding fee is newer than the entry time
                        if funding_time > latest_funding_time:
                            latest_funding_time = funding_time

                            # Execute sell orders immediately after funding fee detection
                            logging.info("\n=== EXIT TRIGGERED ===")
                            await self._execute_sell_plan(async_client)

                            # Log funding fee details AFTER sell execution
                            logging.info(f"Funding fee received: {income}")

                            return  # Exit the loop as funding fee is detected

                # Reduce sleep interval to 10ms for faster detection
                await asyncio.sleep(0.01)

    async def execute_strategy(self):
        async_client = await AsyncClient.create(
            os.getenv('API_KEY'), os.getenv('API_SECRET')
        )

        try:
            await self._verify_connection(async_client)
            await self._set_leverage(async_client)
            await self._calibrate_time_sync(async_client)

            # Entry execution
            entry_target = self._calculate_target(*self.ENTRY_TIME)
            await self._precision_wait(entry_target)
            logging.info("\n=== ENTRY TRIGGERED ===")
            
            # Execute BUY order and capture the entry time
            entry_time = self._get_server_time()  # Capture the trade entry time
            await self._execute_order(async_client, 'BUY')

            # Fetch order book and create sell plan immediately after BUY
            await self._fetch_order_book(async_client)

            # Wait for funding fee to be credited for the current trade
            logging.info("Waiting for funding fee to be credited...")
            await self._wait_for_funding_fee(async_client, entry_time)

        except Exception as e:
            logging.error(f"Strategy failed: {e}")
        finally:
            await async_client.close_connection()


if __name__ == "__main__":
    trader = PrecisionFuturesTrader()
    asyncio.run(trader.execute_strategy())