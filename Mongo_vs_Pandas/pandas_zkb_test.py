"""pandas_zkb_test.py: demo script for timing/testing pandas data transformations"""

from os import path
import json
from datetime import datetime
import math
import warnings

from plumbum import cli
import pandas as pd
import requests

HERE = path.abspath(path.dirname(__file__))

ZKB_URI = 'https://zkillboard.com/api/'

def fetch_zkb_data(
        zkb_query,
        kill_counts,
        zkb_uri=ZKB_URI,
        kills_per_request=200
):
    """fetches raw list data from zkillboard.com

    Notes:
        See ZKB docs for query params
        https://github.com/zKillboard/zKillboard/wiki/API-(Killmails)

    Args:
        zkb_query (str): params for zkb query
        kill_counts (int): number of kills to fetch
        zkb_uri (str, optional): root URI for zkb API fetches
        kills_per_request (int, optional): help figure out pagination

    Returns:
        (:obj:`list`): list of killmail objects

    """
    pages = math.ceil(kill_counts/kills_per_request)
    if pages > 10:
        warnings.warn(
            'ZKB only supports 10 pages on most queries',
            UserWarning
        )

    kills_list = []
    for page in cli.terminal.Progress(range(1,pages+1)):
        req = requests.get(
            '{zkb_uri}{zkb_query}page/{page}/'.format(
                zkb_uri=zkb_uri,
                zkb_query=zkb_query,
                page=page
            )
        )
        req.raise_for_status()
        data = req.json()

        kills_list.extend(data)

    return kills_list


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

    count = 2000
    @cli.switch(
        ['c', '--count'],
        int,
        help='Number of records to grab'
    )
    def override_count(self, count):
        """override expected kill count"""
        self.count = count

    def main(self):
        """project main goes here"""
        print('Hello world')
        raw_kill_list = fetch_zkb_data(
            self.query,
            self.count
        )
        with open('raw_zkb.json', 'w') as raw_fh:
            json.dump(raw_kill_list, raw_fh, indent=2)

if __name__ == '__main__':
    PandasZKBTest.run()
