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
        self.SYMBOL = 'IPUSDT'  # Trading pair
        self.FIXED_QTY = 140  # Quantity
        self.LEVERAGE = 20  # Leverage
        self.ENTRY_TIME = (1, 29, 59, 150)  # 01:29:59.150 AM IST (updated time)
        self.time_offset = 0.0

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
        for _ in range(10):
            try:
                t0 = asyncio.get_event_loop().time()
                server_time = (await async_client.futures_time())['serverTime'] / 1000
                t1 = asyncio.get_event_loop().time()
                measurements.append((t1 - t0, server_time - (t0 + t1) / 2))
            except Exception as e:
                logging.warning(f"Time sync failed: {e}")

        avg_latency = sum(m[0] for m in measurements) / len(measurements)
        self.time_offset = sum(m[1] for m in measurements) / len(measurements)
        logging.info(f"Time synced | Offset: {self.time_offset*1000:.2f}ms | Latency: {avg_latency*1000:.2f}ms")

    async def _set_leverage(self, async_client):
        await async_client.futures_change_leverage(
            symbol=self.SYMBOL,
            leverage=self.LEVERAGE
        )

    def _get_server_time(self):
        return asyncio.get_event_loop().time() + self.time_offset

    def _calculate_target(self, hour, minute, second, millisecond):
        """Calculate the target timestamp in Binance server time"""
        now = datetime.fromtimestamp(self._get_server_time(), IST)
        target = now.replace(
            hour=hour,
            minute=minute,
            second=second,
            microsecond=millisecond * 1000
        )
        return target.timestamp()

    async def _precision_wait(self, target_ts):
        """Enhanced wait with high precision"""
        while True:
            current = self._get_server_time()
            if current >= target_ts:
                return
            remaining = target_ts - current
            await asyncio.sleep(max(remaining * 0.5, 0.001))

    async def _execute_order(self, async_client, side):
        """Execute a market order"""
        try:
            await async_client.futures_create_order(
                symbol=self.SYMBOL,
                side=side,
                type='MARKET',
                quantity=self.FIXED_QTY,
                newOrderRespType='FULL'
            )
            logging.info(f"Market {side} order executed successfully.")
        except Exception as e:
            logging.error(f"Failed to execute market {side} order: {e}")
            raise

    async def _wait_for_funding_fee(self, async_client):
        """Wait for funding fee to be credited with optimized polling"""
        try:
            latest_funding_time = 0

            while True:
                # Fetch funding fee history for the trading pair
                income_history = await async_client.futures_income_history(
                    symbol=self.SYMBOL,
                    incomeType='FUNDING_FEE',
                    limit=1  # Fetch only the latest funding fee record
                )

                if income_history:
                    latest_income = income_history[0]
                    funding_time = latest_income['time'] / 1000  # Convert to seconds

                    # Check if latest funding fee is newer than the stored one
                    if funding_time > latest_funding_time:
                        latest_funding_time = funding_time
                        logging.info(f"Funding fee received: {latest_income}")
                        return  # Exit the loop as funding fee is detected

                # Sleep for 200ms to reduce delay and stay within Binance rate limits
                await asyncio.sleep(0.2)

        except Exception as e:
            logging.error(f"Failed to verify funding fee: {e}")
            raise

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
            await self._execute_order(async_client, 'BUY')

            # Wait for funding fee to be credited
            logging.info("Waiting for funding fee to be credited...")
            await self._wait_for_funding_fee(async_client)

            # Exit execution
            logging.info("\n=== EXIT TRIGGERED ===")
            await self._execute_order(async_client, 'SELL')

        except Exception as e:
            logging.error(f"Strategy failed: {e}")
        finally:
            await async_client.close_connection()  # Close the AsyncClient connection


if __name__ == "__main__":
    trader = PrecisionFuturesTrader()
    asyncio.run(trader.execute_strategy())