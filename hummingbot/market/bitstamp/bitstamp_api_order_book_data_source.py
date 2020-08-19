#!/usr/bin/env python

import asyncio
import aiohttp
import logging
import pandas as pd
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional
)
import time
import ujson
import websockets
from websockets.exceptions import ConnectionClosed

from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.logger import HummingbotLogger
from hummingbot.market.bitstamp.bitstamp_order_book import BitstampOrderBook

ORDER_BOOK_SNAPSHOT_URL = "https://www.bitstamp.net/api/v2/order_book"
TICKER_URL = "https://www.bitstamp.net/api/v2/ticker"
TRADING_PAIRS_URL = "https://www.bitstamp.net/api/v2/trading-pairs-info/"
STREAM_URL = "wss://ws.bitstamp.net"
MAX_RETRIES = 20
NaN = float("nan")


class BitstampAPIOrderBookDataSource(OrderBookTrackerDataSource):
    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _bitstampobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._bitstampobds_logger is None:
            cls._bitstampobds_logger = logging.getLogger(__name__)
        return cls._bitstampobds_logger

    def __init__(self, trading_pairs: Optional[List[str]] = None):
        super().__init__()
        self._trading_pairs: Optional[List[str]] = trading_pairs
        self._order_book_create_function = lambda: OrderBook()

    @classmethod
    @async_ttl_cache(ttl=60 * 30, maxsize=1)
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        """
        Returned data frame should have trading_pair as index and include usd volume, baseAsset and quoteAsset
        """
        async with aiohttp.ClientSession() as client:

            trading_pairs_response = await client.get(TRADING_PAIRS_URL)
            trading_pairs_response: aiohttp.ClientResponse = trading_pairs_response

            if trading_pairs_response.status != 200:
                raise IOError(f"Error fetching Bitstamp markets information. "
                              f"HTTP status is {trading_pairs_response.status}.")

            trading_pairs_data = await trading_pairs_response.json()

            trading_pairs: List[Dict[str, Any]] = [{"pair": item["url_symbol"],
                                                    "baseAsset": item["name"].split("/")[0],
                                                    "quoteAsset": item["name"].split("/")[1]}
                                                   for item in trading_pairs_data
                                                   if item["trading"] == "Enabled"]

            all_markets: pd.DataFrame = pd.DataFrame.from_records(data=trading_pairs, index="pair")

            pairs: List[str] = list(all_markets.index)
            volumes: List[float] = []
            prices: List[float] = []
            for pair in pairs:
                ticker_url: str = f"{TICKER_URL}/{pair}/"
                should_retry: bool = True
                retry_counter: int = 0
                while should_retry:
                    async with client.get(ticker_url) as ticker_response:
                        retry_counter += 1
                        ticker_response: aiohttp.ClientResponse = ticker_response
                        if ticker_response.status == 200:
                            data: Dict[str, Any] = await ticker_response.json()
                            should_retry = False
                            volumes.append(float(data.get("volume", NaN)))
                            prices.append(float(data.get("price", NaN)))
                        elif ticker_response.status != 200 or retry_counter == MAX_RETRIES:
                            raise IOError(f"Error fetching ticker for {pair} on Bitstamp. "
                                          f"HTTP status is {ticker_response.status}.")
                        await asyncio.sleep(0.5)

            all_markets["volume"] = volumes
            all_markets["price"] = prices
            btc_usd_price: float = all_markets.loc["btcusd"].price
            btc_eur_price: float = all_markets.loc["btceur"].price
            btc_gbp_price: float = all_markets.loc["btcgbp"].price
            btc_pax_price: float = all_markets.loc["btcpax"].price
            usd_volume: List[float] = []
            for row in all_markets.itertuples():
                product_name: str = row.Index
                quote_volume: float = row.volume
                quote_price: float = row.price
                if product_name.endswith("usd"):
                    usd_volume.append(quote_volume * quote_price)
                elif product_name.endswith("pax"):
                    usd_volume.append(quote_volume * quote_price * (btc_usd_price / btc_pax_price))
                elif product_name.endswith("btc"):
                    usd_volume.append(quote_volume * quote_price * btc_usd_price)
                elif product_name.endswith("eur"):
                    usd_volume.append(quote_volume * quote_price * (btc_usd_price / btc_eur_price))
                elif product_name.endswith("gbp"):
                    usd_volume.append(quote_volume * quote_price * (btc_usd_price / btc_gbp_price))
                else:
                    usd_volume.append(NaN)
                    cls.logger().error(f"Unable to convert volume to USD for market - {product_name}.")
            all_markets["USDVolume"] = usd_volume
            return all_markets.sort_values("USDVolume", ascending=False)

    async def get_trading_pairs(self) -> List[str]:
        if not self._trading_pairs:
            try:
                active_markets: pd.DataFrame = await self.get_active_exchange_markets()
                self._trading_pairs = active_markets.index.tolist()
            except Exception:
                self._trading_pairs = []
                self.logger().network(
                    "Error getting active exchange information.",
                    exc_info=True,
                    app_warning_msg="Error getting active exchange information. Check network connection."
                )
        return self._trading_pairs

    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str) -> Dict[str, Any]:
        order_book_url: str = f"{ORDER_BOOK_SNAPSHOT_URL}/{trading_pair}/"
        async with client.get(order_book_url) as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching Bitstamp market snapshot for {trading_pair}. "
                              f"HTTP status is {response.status}.")
            data: Dict[str, Any] = await response.json()
            data = {"trading_pair": trading_pair, **data}

            # Need to add the symbol into the snapshot message for the Kafka message queue.
            # Because otherwise, there'd be no way for the receiver to know which market the
            # snapshot belongs to.

            return data

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        # Get the currently active markets
        async with aiohttp.ClientSession() as client:
            trading_pairs: List[str] = await self.get_trading_pairs()
            retval: Dict[str, OrderBookTrackerEntry] = {}

            number_of_pairs: int = len(trading_pairs)
            for index, trading_pair in enumerate(trading_pairs):
                try:
                    snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                    snapshot_timestamp: float = time.time()
                    snapshot_msg: OrderBookMessage = BitstampOrderBook.snapshot_message_from_exchange(
                        snapshot,
                        snapshot_timestamp,
                        metadata={"trading_pair": trading_pair}
                    )
                    order_book: OrderBook = self.order_book_create_function()
                    order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
                    retval[trading_pair] = OrderBookTrackerEntry(trading_pair, snapshot_timestamp, order_book)
                    self.logger().info(f"Initialized order book for {trading_pair}. "
                                       f"{index + 1}/{number_of_pairs} completed.")
                    # Each 1000 limit snapshot costs 10 requests and Binance rate limit is 20 requests per second.
                    await asyncio.sleep(1.0)
                except Exception:
                    self.logger().error(f"Error getting snapshot for {trading_pair}. ", exc_info=True)
                    await asyncio.sleep(5)
            return retval

    async def _inner_messages(self,
                              ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    yield msg
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()

                async with websockets.connect(STREAM_URL) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    for pair in trading_pairs:
                        subscribe_msg: Dict[str, Any] = {
                            "event": "bts:subscribe",
                            "data": {"channel": f"live_trades_{pair}"}
                        }
                        await ws.send(ujson.dumps(subscribe_msg))
                    async for raw_msg in self._inner_messages(ws):
                        msg = ujson.loads(raw_msg)
                        msg_type: str = msg.get("event", None)
                        if msg_type is None:
                            raise ValueError(f"Bitstamp Websocket message does not contain an event type - {msg}")
                        elif msg_type == "trade":
                            trade_msg: OrderBookMessage = BitstampOrderBook.trade_message_from_exchange(
                                msg["data"],
                                metadata={"trading_pair": msg["channel"].split("_")[2]}
                            )
                            output.put_nowait(trade_msg)
                        else:
                            raise ValueError(f"Bitstamp Websocket received event type other then trade in trade listener - {msg}")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()

                async with websockets.connect(STREAM_URL) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    for pair in trading_pairs:
                        subscribe_msg: Dict[str, Any] = {
                            "event": "bts:subscribe",
                            "data": {"channel": f"diff_order_book_{pair}"}
                        }
                        await ws.send(ujson.dumps(subscribe_msg))
                    async for raw_msg in self._inner_messages(ws):
                        msg = ujson.loads(raw_msg)
                        msg_type: str = msg.get("event", None)
                        if msg_type is None:
                            raise ValueError(f"Bitstamp Websocket message does not contain an event type - {msg}")
                        elif msg_type == "data":
                            order_book_message: OrderBookMessage = BitstampOrderBook.diff_message_from_exchange(
                                msg["data"],
                                time.time(),
                                metadata={"trading_pair": msg["channel"].split("_")[2]}
                            )
                            output.put_nowait(order_book_message)
                        else:
                            raise ValueError(
                                f"Bitstamp Websocket received event type other then trade in trade listener - {msg}")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()

                async with websockets.connect(STREAM_URL) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    for pair in trading_pairs:
                        subscribe_msg: Dict[str, Any] = {
                            "event": "bts:subscribe",
                            "data": {"channel": f"order_book_{pair}"}
                        }
                        await ws.send(ujson.dumps(subscribe_msg))
                    async for raw_msg in self._inner_messages(ws):
                        msg = ujson.loads(raw_msg)
                        msg_type: str = msg.get("event", None)
                        if msg_type is None:
                            raise ValueError(f"Bitstamp Websocket message does not contain an event type - {msg}")
                        elif msg_type == "data":
                            order_book_message: OrderBookMessage = BitstampOrderBook.snapshot_message_from_exchange(
                                msg["data"],
                                time.time(),
                                metadata={"trading_pair": msg["channel"].split("_")[2]}
                            )
                            output.put_nowait(order_book_message)
                        else:
                            raise ValueError(
                                f"Bitstamp Websocket received event type other then trade in trade listener - {msg}")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)
