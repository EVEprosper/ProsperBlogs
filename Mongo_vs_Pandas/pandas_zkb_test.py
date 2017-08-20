"""pandas_zkb_test.py: demo script for timing/testing pandas data transformations"""

from os import path
import json
from datetime import datetime
import math
import warnings

from contexttimer import Timer
from plumbum import cli
import pandas as pd
import requests

HERE = path.abspath(path.dirname(__file__))

DICT_KEYS = ['victim', 'position', 'zkb']
LIST_KEYS = ['attackers', 'items']

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
    for page in cli.terminal.Progress(range(1, pages+1)):
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

def pivot_dict_pandas(
        raw_data,
        dict_keys=DICT_KEYS,
        ignore_keys=LIST_KEYS
):
    """pivot data -- dicts

    Note:
        Uses pandas-only approach

    Args:
        raw_data (:obj:`list`): raw zkb data to prase into frame
        dict_keys (:obj:`list`, optional): keys to pivot
        ignore_keys (:obj:`list`, optional): keys to drop

    Returns:
        (:obj:`pandas.DataFrame`): result data

    """
    print('--Parsing with Pandas')
    raw_df = pd.DataFrame(raw_data)

    print('--Dropping ignore_keys')
    raw_df = raw_df.drop(ignore_keys, 1)

    for key in dict_keys:
        temp_df = pd.DataFrame(list(raw_df[key]))
        raw_df = pd.concat([raw_df, temp_df], axis=1)

    print('--Dropping dict_keys')
    raw_df = raw_df.drop(dict_keys, 1)

    return raw_df

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
        print('Fetching raw data from zKillboard')
        raw_kill_list = fetch_zkb_data(
            self.query,
            self.count
        )

        print('Pivoting Data -- Dict keys with Pandas')
        with Timer() as pandas_dict_timer:
            pandas_dict_df = pivot_dict_pandas(raw_kill_list)
            print('--Time Elapsed: {}'.format(pandas_dict_timer))

        pandas_dict_df.to_csv('pandas_dict_df.csv', index=False)

if __name__ == '__main__':
    PandasZKBTest.run()
