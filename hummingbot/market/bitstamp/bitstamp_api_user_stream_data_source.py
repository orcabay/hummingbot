#!/usr/bin/env python

import asyncio
import aiohttp
import logging
from typing import (
    Optional,
)
import time
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.market.bitstamp.bitstamp_auth import BitstampAuth


# BALANCE_URL = "https://www.bitstamp.net/api/v2/balance/"
# TRANSACTIONS_URL = "https://www.bitstamp.net/api/v2/user_transactions/"
# ORDERS_URL = "https://www.bitstamp.net/api/v2/open_orders/all/"
BALANCE_URL = "https://front.clients.stagebts.net/api/v2/balance/"
TRANSACTIONS_URL = "https://front.clients.stagebts.net/api/v2/user_transactions/"
ORDERS_URL = "https://front.clients.stagebts.net/api/v2/open_orders/all/"


MESSAGE_TIMEOUT = 3.0
PING_TIMEOUT = 5.0


class BitstampAPIUserStreamDataSource(UserStreamTrackerDataSource):

    _bstpausds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._bstpausds_logger is None:
            cls._bstpausds_logger = logging.getLogger(__name__)
        return cls._bstpausds_logger

    def __init__(self, bitstamp_auth: BitstampAuth):
        self._bitstamp_auth: BitstampAuth = bitstamp_auth
        self._shared_client: Optional[aiohttp.ClientSession] = None
        self._last_recv_time: float = 0
        super().__init__()

    @property
    def last_recv_time(self):
        return self._last_recv_time

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):

        shared_client: aiohttp.ClientSession = await self._http_client()

        while True:
            async with shared_client as client:

                balance_response = await client.post(BALANCE_URL, data=self._bitstamp_auth.generate_auth_dict())
                balance_response: aiohttp.ClientResponse = balance_response

                if balance_response.status != 200:
                    raise IOError(f"Error fetching Bitstamp user balance. "
                                  f"HTTP status is {balance_response.status}.")

                balance_msg = await balance_response.json()
                output.put_nowait(balance_msg)

                transactions_response = await client.post(TRANSACTIONS_URL, data=self._bitstamp_auth.generate_auth_dict())
                transactions_response: aiohttp.ClientResponse = transactions_response

                if transactions_response.status != 200:
                    raise IOError(f"Error fetching Bitstamp user balance. "
                                  f"HTTP status is {transactions_response.status}.")

                transactions_msg = await transactions_response.json()
                output.put_nowait(transactions_msg)

                orders_response = await client.post(ORDERS_URL, data=self._bitstamp_auth.generate_auth_dict())
                orders_response: aiohttp.ClientResponse = orders_response

                if orders_response.status != 200:
                    raise IOError(f"Error fetching Bitstamp user balance. "
                                  f"HTTP status is {orders_response.status}.")

                orders_msg = await orders_response.json()
                output.put_nowait(orders_msg)

                self._last_recv_time = time.time()

                await asyncio.sleep(1.0)

    async def _http_client(self) -> aiohttp.ClientSession:
        if self._shared_client is None or self._shared_client.closed:
            self._shared_client = aiohttp.ClientSession()
        return self._shared_client

    async def stop(self):
        if self._shared_client is not None and not self._shared_client.closed:
            await self._shared_client.close()
