from itertools import groupby
import json
import logging
from operator import itemgetter
import os
import os.path as op
from pprint import pprint, pformat
import re
import shutil
import sys
from textwrap import fill
import urllib.parse as urlparse

import click
import globus_sdk
import requests as rq
from terminaltables import SingleTable, AsciiTable


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


BASE_URL = 'https://alyx-dev.cortexlab.net/'
BASE_URL = 'http://localhost:8000/'  # test
CONFIG_PATH = '~/.alyx'
GLOBUS_CLIENT_ID = '525cc517-8ccb-4d11-8036-af332da5eafd'
TABLE_WIDTH = '{0: <72}'


def get_config_path(path=''):
    path = op.expanduser(op.join(CONFIG_PATH, path))
    os.makedirs(op.dirname(path), exist_ok=True)
    return path


def get_token_path():
    return get_config_path('alyx-token.json')


def write_token(data):
    with open(get_token_path(), 'w') as f:
        json.dump(data, f, indent=2, sort_keys=True)
    token = data.get('token', None)
    logger.debug(f"Write token {token}.")
    return token


def get_token():
    path = get_token_path()
    if not op.exists(path):
        return
    with open(path, 'r') as f:
        token = json.load(f).get('token', '')
    logger.debug(f"Read token {token}.")
    return token


def _extract_uuid(url):
    return re.search(r'\/([a-zA-Z0-9\-]+)$', url).group(1)


def _pp(value):
    if isinstance(value, list):
        out = '\n'.join((_simple_table(row)) for row in value)
        return '\n'.join(TABLE_WIDTH.format(line) for line in out.splitlines())
    else:
        return TABLE_WIDTH.format(str(value))


def _simple_table(data):
    assert isinstance(data, dict)
    table = [[key, _pp(value)] for key, value in data.items()]
    st = AsciiTable(table)
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
        st = AsciiTable(table)
        for item in data:
            table.append([item[key] for key in keys])
        st.inner_heading_row_border = False
        if st.table_width <= twidth:
            return st.table
        else:
            return '\n\n'.join(get_table(item) for item in data)


class AlyxClient:
    _token = ''

    def __init__(self):
        self._token = get_token()

    def _make_end_point(self, path=''):
        if path.startswith('/'):
            path = path[1:]
        return BASE_URL + path

    def _request(self, url, method, **kwargs):
        if not url.startswith('http'):
            url = self._make_end_point(url)
        for i in range(1):
            if self._token:
                kwargs['headers'] = {'Authorization': 'Token ' + self._token}
            logger.debug(f"{method.upper()} request to {url} with data {kwargs}")
            resp = getattr(rq, method)(url, **kwargs)
            if resp.status_code == 403:
                self._clear_token()
                self._auto_auth()
            elif resp.status_code in (200, 201):
                return resp
        raise Exception(resp.text)

    def get(self, url, **data):
        if data:
            url = url + '?' + '&'.join(f'{key}={value}' for key, value in data.items())
        return self._process_response(self._request(url, 'get'))

    def post(self, url, **data):
        return self._process_response(self._request(url, 'post', data=data))

    def put(self, url, **data):
        return self._process_response(self._request(url, 'put', data=data))

    def patch(self, url, **data):
        return self._process_response(self._request(url, 'patch', data=data))

    def _clear_token(self):
        self._token = None
        path = get_token_path()
        if op.exists(path):
            logger.debug(f"Remove token at {path}")
            os.remove(path)

    def _process_response(self, resp):
        if resp and resp.status_code in (200, 201):
            output = resp.text
            # output = output.replace(BASE_URL, '/')
            return json.loads(output)
        else:
            raise Exception(resp)

    def _auth(self, username, password):
        url = self._make_end_point('/auth-token/')
        resp = self.post(url, username=username, password=password)
        if not resp:
            return
        return write_token(resp)

    def _auto_auth(self):
        # Open credentials, a text file in '.' with just <username>:<password>
        with open(op.expanduser('~/.alyx/credentials'), 'r') as f:
            username, password = f.read().strip().split(':')
        # This command saves a ~/.alyx/auth-token.json file with a token.
        self._auth(username, password)
        self._token = get_token()


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
    files = files[-10:]
    for dataset, missing_files in groupby(files, itemgetter('dataset')):
        existing_files = c.get('/files', dataset=_extract_uuid(dataset), exists=True)
        if not existing_files:
            continue
        existing_file = existing_files[0]
        for missing_file in missing_files:
            assert existing_file['exists']
            assert not missing_file['exists']
            yield {
                'dataset': dataset,
                'source_data_repository': existing_file['data_repository'],
                'destination_data_repository': missing_file['data_repository'],
                'source_relative_path': existing_file['relative_path'],
                'destination_relative_path': missing_file['relative_path'],
                'source_file_record': _extract_uuid(existing_file['url']),
                'destination_file_record': _extract_uuid(missing_file['url']),
            }


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


def start_globus_transfer(source_file_id, destination_file_id, dry_run=False):
    """Start a globus file transfer between two file record UUIDs."""
    c = AlyxClient()

    source_file_record = c.get('/files/' + source_file_id)
    destination_file_record = c.get('/files/' + destination_file_id)

    source_repo = source_file_record['data_repository']
    destination_repo = destination_file_record['data_repository']

    source_id = c.get('/data-repository/' + source_repo)['globus_endpoint_id']
    destination_id = c.get('/data-repository/' + destination_repo)['globus_endpoint_id']

    if not source_id and not destination_id:
        raise Exception("The Globus endpoint ids of source and destination must be set.")

    source_path = source_file_record['relative_path']
    destination_path = destination_file_record['relative_path']

    tc = globus_transfer_client()
    tdata = globus_sdk.TransferData(
        tc, source_id, destination_id, verify_checksum='checksum', sync_level='checksum',
    )
    tdata.add_item(source_path, destination_path)

    logger.debug(tdata)

    dry_run = True
    if not dry_run:
        return tc.submit_transfer(tdata)


@alyx.command()
@click.argument('source', required=False, metavar='source_file_record_uuid')
@click.argument('destination', required=False, metavar='destination_file_record_uuid')
@click.option('--all', is_flag=True, help='Process all missing file records')
@click.option('--dataset', help='Process all missing file records of a particular dataset')
@click.option('--dry-run', is_flag=True,
              help='Just display the transfers instead of launching them')
@click.pass_context
def transfer(ctx, source=None, destination=None, all=False, dataset=None, dry_run=False):
    if source and destination:
        start_globus_transfer(source, destination, dry_run=dry_run)
        return

    for dset in transfers_required(dataset):
        print(dset)
        start_globus_transfer(dset['source_file_record'],
                              dset['destination_file_record'],
                              dry_run=dry_run)


if __name__ == '__main__':
    alyx(obj={})
