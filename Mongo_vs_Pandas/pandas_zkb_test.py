"""pandas_zkb_test.py: demo script for timing/testing pandas data transformations"""

from os import path
from datetime import datetime

from plumbum import cli
import pandas as pd
import requests

ZKB_URI = 'https://zkillboard.com/api/'
class PandasZKBTest(cli.Application):
    """Process zKillboard data into pandas with various transformation techniques"""

    debug = cli.Flag(
        ['d', '--debug'],
        help='Enable debug mode'
    )

    query = 'w-space/losses/'
    @cli.switch(
        ['q', '--query'],
        str,
        help='zkb query -- https://github.com/zKillboard/zKillboard/wiki/API-(Killmails)')
    def override_query(self, zkb_query):
        """override zkb query"""
        self.query = zkb_query

    def main(self):
        """project main goes here"""
        print('Hello world')

if __name__ == '__main__':
    PandasZKBTest.run()
