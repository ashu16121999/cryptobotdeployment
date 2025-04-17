import asyncio
import logging
from datetime import datetime, timedelta, timezone
import os
from binance import AsyncClient, BinanceSocketManager
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Configure logging
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


class FundingFeeBot:
    def __init__(self):
        self.SYMBOL = 'AERGOUSDT'
        self.FIXED_QTY = 700  # Quantity to trade
        self.LEVERAGE = 6  # Leverage
        self.ENTRY_TIME = (17, 29, 59, 500)  # 05:29:59:500 PM IST
        self.TIMEOUT_SECONDS = 10  # Safety timeout for funding fee detection
        self.time_offset = 0.0
        self.entry_time = None
        self.buy_timestamp = None  # Exact timestamp of buy execution
        self.sell_timestamp = None  # Exact timestamp of sell execution
        self.funding_timestamp = None  # Exact timestamp of funding fee detection
        self.funding_detected = False

    async def _calibrate_time_sync(self, async_client):
        """Synchronize local time with Binance server time."""
        measurements = []
        for _ in range(20):  # Perform multiple measurements
            try:
                t0 = time.time() * 1000  # Local time in ms
                server_time = (await async_client.futures_time())['serverTime']  # Binance server time in ms
                t1 = time.time() * 1000  # Local time in ms
                latency = t1 - t0
                offset = server_time - ((t0 + t1) / 2)  # Offset calculation
                measurements.append((latency, offset))
            except Exception as e:
                logging.warning(f"Time sync failed: {e}")

        avg_latency = sum(m[0] for m in measurements) / len(measurements) if measurements else 0
        self.time_offset = sum(m[1] for m in measurements) / len(measurements) if measurements else 0
        logging.info(f"Time synced | Offset: {self.time_offset:.2f}ms | Latency: {avg_latency:.2f}ms")

    def _get_server_time(self):
        """Get current server time based on synchronized offset."""
        return time.time() * 1000 + self.time_offset

    def _calculate_target(self, hour, minute, second, millisecond):
        """Calculate the target timestamp for a specific time."""
        now = datetime.fromtimestamp(self._get_server_time() / 1000, IST)
        target = now.replace(
            hour=hour,
            minute=minute,
            second=second,
            microsecond=millisecond * 1000
        )
        return target.timestamp() * 1000  # Convert to milliseconds

    async def _precision_wait(self, target_ts):
        """Wait until the precise target timestamp."""
        while True:
            current = self._get_server_time()
            if current >= target_ts:
                return
            remaining = target_ts - current
            await asyncio.sleep(max(remaining / 2000, 0.001))  # Sleep for half the remaining time or minimum 1ms

    async def _execute_order(self, async_client, side, quantity=None):
        """Execute a market order."""
        try:
            qty = quantity if quantity else self.FIXED_QTY
            order = await async_client.futures_create_order(
                symbol=self.SYMBOL,
                side=side,
                type='MARKET',
                quantity=qty,
                newOrderRespType='FULL'
            )
            timestamp = self._get_server_time()
            if side == 'BUY':
                self.buy_timestamp = timestamp
            elif side == 'SELL':
                self.sell_timestamp = timestamp
            logging.info(f"Market {side} order executed successfully for {qty} contracts.")
            return order
        except Exception as e:
            logging.error(f"Failed to execute market {side} order: {e}")
            raise

    async def _monitor_funding_fee_via_websocket(self, async_client):
        """Monitor funding fee using WebSocket."""
        try:
            bsm = BinanceSocketManager(async_client)
            user_socket = bsm.futures_user_socket()

            async with user_socket as stream:
                async for message in stream:
                    if message['e'] == 'INCOME' and message['incomeType'] == 'FUNDING_FEE':
                        self.funding_timestamp = self._get_server_time()
                        self.funding_detected = True
                        await self._execute_order(async_client, 'SELL')  # Exit the position immediately
                        return
        except Exception as e:
            logging.error(f"WebSocket error: {e}. Falling back to REST API polling...")
            await self._fallback_polling(async_client)  # Immediately fallback to polling

    async def _fallback_polling(self, async_client):
        """Fallback method: Poll funding fee using REST API every 10ms."""
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
                        self.funding_timestamp = self._get_server_time()
                        self.funding_detected = True
                        await self._execute_order(async_client, 'SELL')  # Exit the position immediately
                        return
                await asyncio.sleep(0.01)  # Poll every 10ms
        except Exception as e:
            logging.error(f"REST API polling error: {e}")

    async def run(self):
        """Main execution flow."""
        async_client = await AsyncClient.create(
            os.getenv('API_KEY'), os.getenv('API_SECRET')
        )
        try:
            # Synchronize time with Binance server
            await self._calibrate_time_sync(async_client)

            # Calculate entry time
            entry_ts = self._calculate_target(*self.ENTRY_TIME)

            # Wait until the precise entry time
            logging.info("Waiting for the entry time...")
            await self._precision_wait(entry_ts)

            # Execute market buy order
            logging.info("Placing market buy order...")
            await self._execute_order(async_client, 'BUY')
            self.entry_time = self._get_server_time()  # Record entry time

            # Start WebSocket monitoring
            logging.info("Starting WebSocket monitoring...")
            await self._monitor_funding_fee_via_websocket(async_client)

            # Log exact timestamps after sell order is executed
            if self.funding_detected:
                buy_time = datetime.fromtimestamp(self.buy_timestamp / 1000, IST).strftime('%H:%M:%S:%f')[:-3]
                sell_time = datetime.fromtimestamp(self.sell_timestamp / 1000, IST).strftime('%H:%M:%S:%f')[:-3]
                funding_time = datetime.fromtimestamp(self.funding_timestamp / 1000, IST).strftime('%H:%M:%S:%f')[:-3]
                logging.info(f"Exact Timestamps:\n  Buy Executed At: {buy_time}\n  Sell Executed At: {sell_time}\n  Funding Fee Received At: {funding_time}")

        except Exception as e:
            logging.error(f"Error during execution: {e}")
        finally:
            await async_client.close_connection()


if __name__ == "__main__":
    bot = FundingFeeBot()
    asyncio.run(bot.run())