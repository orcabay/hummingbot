#!/usr/bin/env python
from os.path import join, realpath
import sys; sys.path.insert(0, realpath(join(__file__, "../../../")))

from hummingbot.market.bitstamp.bitstamp_api_user_stream_data_source import BitstampAPIUserStreamDataSource
from hummingbot.market.bitstamp.bitstamp_auth import BitstampAuth
import asyncio
import logging
import unittest
import conf


class BitstampAPIUserStreamDataSourceUnitTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.bitstamp_auth = BitstampAuth(conf.bitstamp_client_id.strip(), conf.bitstamp_api_key.strip(), conf.bitstamp_secret_key.strip())
        cls.user_stream_data_source: BitstampAPIUserStreamDataSource = BitstampAPIUserStreamDataSource(
            bitstamp_auth=cls.bitstamp_auth
        )

    def run_async(self, task):
        return self.ev_loop.run_until_complete(task)

    def test_listen_for_user_stream(self):
        user_stream: asyncio.Queue = asyncio.Queue()

        self.run_async(self.user_stream_data_source.listen_for_user_stream(self.ev_loop, user_stream))
        self.run_async(self.user_stream_data_source.stop())

        print(user_stream)


def main():
    logging.basicConfig(level=logging.INFO)
    unittest.main()


if __name__ == "__main__":
    main()
