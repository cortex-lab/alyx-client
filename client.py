from itertools import groupby
import json
import logging
from operator import itemgetter
import os
import os.path as op
from pprint import pprint
import re
import shutil
import sys
from textwrap import fill
import urllib.parse as urlparse

import click
import globus_sdk
import requests as rq
from terminaltables import SingleTable


# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


BASE_URL = 'http://alyx-dev.cortexlab.net/'
BASE_URL = 'http://localhost:8000/'  # test
CONFIG_PATH = '~/.alyx'
GLOBUS_CLIENT_ID = '525cc517-8ccb-4d11-8036-af332da5eafd'


def get_config_path(path=''):
    path = op.expanduser(op.join(CONFIG_PATH, path))
    os.makedirs(op.dirname(path), exist_ok=True)
    return path


def get_token_path():
    return get_config_path('alyx-token.json')


def write_token(data):
    with open(get_token_path(), 'w') as f:
        json.dump(data, f, indent=2, sort_keys=True)
    return data.get('token', None)


def get_token():
    path = get_token_path()
    if not op.exists(path):
        return
    with open(path, 'r') as f:
        return json.load(f).get('token', '')


def is_authenticated():
    return True if get_token() else False


def _extract_uuid(url):
    return re.search(r'\/([a-zA-Z0-9\-]+)$', url).group(1)


def _simple_table(data):
    assert isinstance(data, dict)
    table = [[key, '{0: <72}'.format(fill(str(value), 72))] for key, value in data.items()]
    st = SingleTable(table)
    st.inner_heading_row_border = False
    return st.table


def get_table(data):
    if not data:
        return ''
    tsize = shutil.get_terminal_size((80, 20))
    twidth = tsize.columns
    if isinstance(data, dict):
        return _simple_table(data)
    elif isinstance(data, list):
        keys = data[0].keys()
        # Display the header and first item, and check whether it fits in the terminal.
        # If it does not, the output cannot be displayed in a single table, but every item
        # will be displayed in its own little table.
        table = [[key for key in keys]]
        if data:
            table.append([data[0][key] for key in keys])
        st = SingleTable(table)
        if st.table_width <= twidth:
            for item in data:
                table.append([item[key] for key in keys])
            st.inner_heading_row_border = False
            return st.table
        else:
            return '\n\n'.join(get_table(item) for item in data)


class AlyxClient:
    _token = ''

    def __init__(self):
        if not is_authenticated():
            # Open credentials, a text file in '.' with just <username>:<password>
            with open(op.expanduser('~/.alyx/credentials'), 'r') as f:
                username, password = f.read().strip().split(':')
            # This command saves a ~/.alyx/auth-token.json file with a token.
            self.auth(username, password)
        self._token = get_token()

    def _make_end_point(self, path=''):
        if path.startswith('/'):
            path = path[1:]
        return BASE_URL + path

    def _request(self, url, method, **kwargs):
        if not url.startswith('http'):
            url = self._make_end_point(url)
        if self._token:
            kwargs['headers'] = {'Authorization': 'Token ' + self._token}
        logger.debug(f"{method.upper()} request to {url} with data {kwargs}")
        resp = getattr(rq, method)(url, **kwargs)
        if resp.status_code == 200:
            output = resp.text
            # output = output.replace(BASE_URL, '/')
            return json.loads(output)
        else:
            print(resp)

    def get(self, url, **data):
        if data:
            url = url + '?' + '&'.join(f'{key}={value}' for key, value in data.items())
        return self._request(url, 'get')

    def post(self, url, **data):
        return self._request(url, 'post', data=data)

    def put(self, url, **data):
        return self._request(url, 'put', data=data)

    def patch(self, url, **data):
        return self._request(url, 'patch', data=data)

    def auth(self, username, password):
        if is_authenticated():
            return
        url = self._make_end_point('/auth-token/')
        resp = self.post(url, username=username, password=password)
        if not resp:
            return
        return write_token(resp)


@click.group()
@click.pass_context
def alyx(ctx):
    ctx.obj['client'] = AlyxClient()

    """
    tool ls --session=...  # list datasets
            --status=pending/uploading/done
        <dataset>    date    ...    file_record_1  file_record_2   status
    tool upload --status=pending
    """

    pass


def _request(name, ctx, path, kvpairs):
    data = {}
    for kvpair in kvpairs:
        i = kvpair.index('=')
        key, value = kvpair[:i], kvpair[i + 1:]
        data[key] = value
    client = ctx.obj['client']
    click.echo(get_table(getattr(client, name)(path, **data)))


@alyx.command()
@click.argument('path')
@click.argument('kvpairs', nargs=-1)
@click.pass_context
def get(ctx, path, kvpairs):
    return _request('get', ctx, path, kvpairs)


@alyx.command()
@click.argument('path')
@click.argument('kvpairs', nargs=-1)
@click.pass_context
def post(ctx, path, kvpairs):
    return _request('post', ctx, path, kvpairs)


@alyx.command()
@click.argument('path')
@click.argument('kvpairs', nargs=-1)
@click.pass_context
def put(ctx, path, kvpairs):
    return _request('put', ctx, path, kvpairs)


@alyx.command()
@click.argument('path')
@click.argument('kvpairs', nargs=-1)
@click.pass_context
def patch(ctx, path, kvpairs):
    return _request('patch', ctx, path, kvpairs)


def transfers_required(dataset=None):
    c = AlyxClient()
    if not dataset:
        files = c.get('/files', exists=False)
    else:
        files = c.get('/files', exists=False, dataset=dataset)
    files = sorted(files, key=itemgetter('dataset'))
    for dataset, missing_files in groupby(files, itemgetter('dataset')):
        existing_files = c.get('/files', dataset=_extract_uuid(dataset), exists=True)
        if not existing_files:
            continue
        existing_file = existing_files[0]
        for missing_file in missing_files:
            assert existing_file['exists']
            assert not missing_file['exists']
            o = {}
            for k, v in existing_file.items():
                o['existing_' + k] = v
            for k, v in missing_file.items():
                o['missing_' + k] = v
            yield o


def create_globus_client():
    client = globus_sdk.NativeAppAuthClient(GLOBUS_CLIENT_ID)
    client.oauth2_start_flow(refresh_tokens=True)
    return client


def create_globus_token():
    client = create_globus_client()
    print('Please go to this URL and login: {0}'
          .format(client.oauth2_get_authorize_url()))
    get_input = getattr(__builtins__, 'raw_input', input)
    auth_code = get_input('Please enter the code here: ').strip()
    token_response = client.oauth2_exchange_code_for_tokens(auth_code)
    globus_transfer_data = token_response.by_resource_server['transfer.api.globus.org']

    data = dict(transfer_rt=globus_transfer_data['refresh_token'],
                transfer_at=globus_transfer_data['access_token'],
                expires_at_s=globus_transfer_data['expires_at_seconds'],
                )
    path = get_config_path('globus-token.json')
    with open(path, 'w') as f:
        json.dump(data, f, indent=2, sort_keys=True)


def get_globus_transfer_rt():
    path = get_config_path('globus-token.json')
    if not op.exists(path):
        return
    with open(path, 'r') as f:
        return json.load(f).get('transfer_rt', None)


def globus_transfer_client():
    transfer_rt = get_globus_transfer_rt()
    if not transfer_rt:
        create_globus_token()
        transfer_rt = get_globus_transfer_rt()
    client = create_globus_client()
    authorizer = globus_sdk.RefreshTokenAuthorizer(transfer_rt, client)
    tc = globus_sdk.TransferClient(authorizer=authorizer)
    return tc


def start_globus_transfer(source_id, destination_id, path):
    tc = globus_transfer_client()
    # source_file, destination_file
    tdata = globus_sdk.TransferData(tc,
                                    source_id,
                                    destination_id,
                                    verify_checksum='checksum',
                                    sync_level="checksum",
                                    )
    tdata.add_item(path, path)
    return tc.submit_transfer(tdata)


if __name__ == '__main__':
    alyx(obj={})
