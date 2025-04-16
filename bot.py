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

    async def execute_strategy(self):
        try:
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

                # Execute BUY order
                await self._execute_order(async_client, 'BUY')

            except Exception as e:
                logging.error(f"Strategy execution failed: {e}")
            finally:
                await async_client.close_connection()

        except Exception as e:
            logging.error(f"Failed to initialize API client: {e}")


if __name__ == "__main__":
    trader = PrecisionFuturesTrader()
    asyncio.run(trader.execute_strategy())