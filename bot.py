from binance import AsyncClient, BinanceSocketManager
import os
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot_debug.log'),
        logging.StreamHandler()
    ]
)

IST = timezone(timedelta(hours=5, minutes=30))  # Correct timezone for IST

class PrecisionFuturesTrader:
    def __init__(self):
        self.SYMBOL = 'FUNUSDT'  # Updated coin name
        self.FIXED_QTY = 80000  # Updated quantity
        self.LEVERAGE = 20  # Updated leverage
        # Updated times for 05:29:59.150 PM and 05:30:00.050 PM IST
        self.ENTRY_TIME = (17, 29, 59, 150)  # 05:29:59.150 PM
        self.EXIT_TIME = (17, 30, 0, 50)     # 05:30:00.050 PM
        self.time_offset = 0.0
        self.order_chunks = 1
        self.executed_orders = []

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

    async def _adjust_chunks(self, async_client):
        """Dynamically adjust chunk size based on order book depth"""
        try:
            depth = await async_client.futures_order_book(symbol=self.SYMBOL)
            bids = depth['bids']
            order_book_depth = sum(float(bid[1]) for bid in bids)  # Summing bid quantities

            if order_book_depth > 200:
                self.order_chunks = 1  # Execute as a single order
            elif 100 <= order_book_depth <= 200:
                self.order_chunks = 3  # Split into 2-3 chunks
            else:
                self.order_chunks = 5  # Split into 5+ chunks

            logging.info(f"Order book depth: {order_book_depth} | Chunks adjusted to: {self.order_chunks}")

        except Exception as e:
            logging.error(f"Failed to adjust chunks: {e}")

    async def _precision_wait(self, target_ts):
        """Enhanced wait with high precision"""
        while True:
            current = self._get_server_time()
            if current >= target_ts:
                return
            remaining = target_ts - current
            await asyncio.sleep(max(remaining * 0.5, 0.001))

    async def _execute_split_orders(self, async_client, side):
        """Execute orders in chunks with a small delay"""
        chunk_size = self.FIXED_QTY // self.order_chunks
        for i in range(self.order_chunks):
            try:
                await async_client.futures_create_order(
                    symbol=self.SYMBOL,
                    side=side,
                    type='MARKET',
                    quantity=chunk_size,
                    newOrderRespType='FULL'
                )
                logging.info(f"Chunk {i+1}/{self.order_chunks} executed")
                await asyncio.sleep(0.05)  # 50ms between chunks
            except Exception as e:
                logging.error(f"Chunk {i+1} failed: {e}")

    async def execute_strategy(self):
        async_client = await AsyncClient.create(
            os.getenv('API_KEY'), os.getenv('API_SECRET')
        )  # Await the AsyncClient creation

        try:
            bsm = BinanceSocketManager(async_client)
            await self._verify_connection(async_client)
            await self._set_leverage(async_client)
            await self._calibrate_time_sync(async_client)
            await self._adjust_chunks(async_client)  # Dynamically adjust chunks based on order book depth

            # Entry execution
            entry_target = self._calculate_target(*self.ENTRY_TIME)
            await self._precision_wait(entry_target)
            logging.info("\n=== ENTRY TRIGGERED ===")
            await self._execute_split_orders(async_client, 'BUY')

            # Exit execution
            exit_target = self._calculate_target(*self.EXIT_TIME)
            await self._precision_wait(exit_target)
            logging.info("\n=== EXIT TRIGGERED ===")
            await self._execute_split_orders(async_client, 'SELL')

        except Exception as e:
            logging.error(f"Strategy failed: {e}")
        finally:
            await async_client.close_connection()  # Close the AsyncClient connection

if __name__ == "__main__":
    trader = PrecisionFuturesTrader()
    asyncio.run(trader.execute_strategy())