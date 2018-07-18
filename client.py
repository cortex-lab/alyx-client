from getpass import getpass
from itertools import groupby
import functools
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


logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


BASE_URL = 'https://alyx-dev.cortexlab.net/'
BASE_URL = 'http://localhost:8000/'  # DEBUG
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
    if url is None:
        return None
    if 'http' in url:
        return re.search(r'\/([a-zA-Z0-9\-]+)$', url).group(1)
    else:
        return url


def _pp(value):
    if isinstance(value, dict):
        value = [value]
    if isinstance(value, list):
        out = '\n'.join((_simple_table(row)) for row in value)
        return '\n'.join(TABLE_WIDTH.format(line) for line in out.splitlines())
    else:
        return TABLE_WIDTH.format(str(value))


def _simple_table(data):
    if isinstance(data, str):
        return data
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
        table = [[key for key in keys]]
        st = AsciiTable(table)
        for item in data:
            table.append([_pp(item[key]) for key in keys])
        st.inner_heading_row_border = False
        if st.table_width <= twidth:
            return st.table
        else:
            # If the table does not fit on the terminal, display a single table per item.
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
        for i in range(2):
            if self._token:
                kwargs['headers'] = {'Authorization': 'Token ' + self._token}
            logger.debug(f"{method.upper()} request to {url} with data {kwargs}")
            resp = getattr(rq, method)(url, **kwargs)
            if resp.status_code == 403:
                self._clear_token()
                self._auto_auth()
            elif resp.status_code in (200, 201):
                return resp
            elif resp.status_code == 404:
                raise Exception("The REST endpoint %s doesn't exist." % url)
            else:
                raise Exception(resp.text)
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
        url = self._make_end_point('/auth-token')
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
@click.option('--raw', is_flag=True)
@click.pass_context
def alyx(ctx, raw=False):
    ctx.obj['client'] = AlyxClient()
    ctx.obj['raw'] = raw


def _request(name, ctx, path, kvpairs):
    data = {}
    for kvpair in kvpairs:
        i = kvpair.index('=')
        key, value = kvpair[:i], kvpair[i + 1:]
        data[key] = value
    client = ctx.obj['client']
    out = getattr(client, name)(path, **data)
    if ctx.obj.get('raw', None):
        click.echo(out)
    else:
        click.echo(get_table(out))


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


def _get_files(c, dataset=None, exists=None):
    if not dataset:
        files = c.get('/files', exists=exists)
    else:
        files = c.get('/files', exists=exists, dataset=dataset)
    files = sorted(files, key=itemgetter('dataset'))
    return files


DATA_REPOSITORIES = {}


def _get_data_repository(c, f):
    repo = f['data_repository']
    if repo not in DATA_REPOSITORIES:
        DATA_REPOSITORIES[repo] = c.get('/data-repository/' + repo)
    return DATA_REPOSITORIES[repo]


def transfers_required(dataset=None):
    c = AlyxClient()
    files = _get_files(c, dataset=dataset, exists=False)
    for dataset, missing_files in groupby(files, itemgetter('dataset')):
        existing_files = c.get('/files', dataset=_extract_uuid(dataset), exists=True)
        if not existing_files:
            continue
        # If possible, choose a file from a non-personal server.
        existing_file = next(
            (f for f in existing_files if _get_data_repository(c, f)['globus_is_personal'] is False),
            None)
        if existing_file is None:
            # Otherwise, fallback on a personal server. But the destination should not
            # be a personal server as well.
            existing_file = existing_files[0]
        for missing_file in missing_files:
            assert existing_file['exists']
            assert not missing_file['exists']
            # WARNING: we should check that the destination data_repository is not personal if
            # the source repository is personal.
            if (_get_data_repository(c, missing_file)['globus_is_personal'] and
                    _get_data_repository(c, existing_file)['globus_is_personal']):
                logger.warn("Our globus subscription does not support file transfer between two "
                            "personal servers.")
                continue
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


def _escape_label(label):
    return re.sub(r'[^a-zA-Z0-9 \-]', '-', label)


def start_globus_transfer(source_file_id, destination_file_id, dry_run=False):
    """Start a globus file transfer between two file record UUIDs."""
    c = AlyxClient()

    source_file_record = c.get('/files/' + source_file_id)
    destination_file_record = c.get('/files/' + destination_file_id)

    source_repo = source_file_record['data_repository']
    destination_repo = destination_file_record['data_repository']

    source_repo_obj = _get_data_repository(c, source_file_record)
    destination_repo_obj = _get_data_repository(c, destination_file_record)

    source_id = source_repo_obj['globus_endpoint_id']
    destination_id = destination_repo_obj['globus_endpoint_id']

    if not source_id and not destination_id:
        raise Exception("The Globus endpoint ids of source and destination must be set.")

    source_path = source_file_record['relative_path']
    destination_path = destination_file_record['relative_path']

    source_path = op.join(source_repo_obj['path'], source_path)
    destination_path = op.join(destination_repo_obj['path'], destination_path)

    label = 'Transfer %s %s to %s %s' % (
        source_repo,
        _escape_label(source_path),
        destination_repo,
        _escape_label(destination_path),
    )
    tc = globus_transfer_client()
    tdata = globus_sdk.TransferData(
        tc, source_id, destination_id, verify_checksum=True, sync_level='checksum',
        label=label,
    )
    tdata.add_item(source_path, destination_path)

    logger.info("Transfer from %s <%s> to %s <%s>%s.",
                source_repo, source_path, destination_repo, destination_path,
                ' (dry)' if dry_run else '')

    if dry_run:
        return

    response = tc.submit_transfer(tdata)

    task_id = response.get('task_id', None)
    message = response.get('message', None)
    code = response.get('code', None)

    logger.info("%s (task UUID: %s)", message, task_id)
    return response


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

    if dataset is None:
        assert all is not None
    dataset = _extract_uuid(dataset)
    for file in transfers_required(dataset):
        start_globus_transfer(file['source_file_record'],
                              file['destination_file_record'],
                              dry_run=dry_run)


@alyx.command()
@click.argument('dataset', required=False)
@click.option('--all', is_flag=True, help='Process all missing file records')
@click.option('--dry-run', is_flag=True,
              help='Just display the actions instead of executing them')
@click.pass_context
def sync(ctx, dataset=None, all=None, dry_run=False):
    # Need to use --all if no dataset is provided.
    if dataset is None:
        assert all is not None
    dataset = _extract_uuid(dataset)

    c = AlyxClient()
    tc = globus_transfer_client()

    files = _get_files(c, dataset=dataset, exists=False)
    for dataset, missing_files in groupby(files, itemgetter('dataset')):
        for file in missing_files:
            name = op.basename(file['relative_path'])
            repo_obj = _get_data_repository(c, file)
            globus_id = repo_obj['globus_endpoint_id']
            # List all files on the endpoint in that directory.
            path = op.join(repo_obj['path'], op.dirname(file['relative_path']))
            try:
                existing = tc.operation_ls(globus_id, path=path)
            except globus_sdk.exc.TransferAPIError as e:
                logger.warn(e)
                continue
            for existing_file in existing:
                if existing_file['name'] == name and existing_file['size'] > 0:
                    # If the file exists and is not empty, we update alyx.
                    logger.info("File record exists on %s, updating alyx%s.",
                                (file['data_repository'], ' (dry)' if dry_run else ''))
                    if not dry_run:
                        c.patch('/files/' + file['id'], exists=True)
                        click.echo(_simple_table(c.get('/files/' + file['id'])))
                    continue


@alyx.command()
@click.argument('task_id', required=True, metavar='task_id')
@click.pass_context
def status(ctx, task_id):
    tc = globus_transfer_client()
    result = tc.get_task(task_id)
    keys = ('status,label,source_endpoint_display_name,destination_endpoint_display_name,'
            'request_time,completion_time,files,bytes_transferred').split(',')
    click.echo(_simple_table({k: result[k] for k in keys}))


@alyx.command()
@click.pass_context
def login(ctx):
    if click.confirm("Logging to Alyx?"):
        username = click.prompt("Alyx username")
        password = getpass("Alyx password:")
        with open(op.expanduser('~/.alyx/credentials'), 'w') as f:
            f.write(f'{username}:{password}')
    if click.confirm("Logging to Globus?"):
        create_globus_token()


if __name__ == '__main__':
    alyx(obj={})
