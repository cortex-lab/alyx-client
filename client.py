import json
import os
import os.path as op
from pprint import pprint
import shutil
import sys
from textwrap import fill

import click
import requests as rq
from terminaltables import SingleTable


BASE_URL = 'http://alyx-dev.cortexlab.net/'
BASE_URL = 'http://localhost:8000/'  # test
CONFIG_PATH = '~/.alyx'


def get_config_path(path=''):
    path = op.expanduser(op.join(CONFIG_PATH, path))
    os.makedirs(op.dirname(path), exist_ok=True)
    return path


def get_token_path():
    return get_config_path('auth-token.json')


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


def _simple_table(data):
    assert isinstance(data, dict)
    table = [[key, '{0: <50}'.format(fill(str(value), 50))] for key, value in data.items()]
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
        table = [[key for key in keys]]
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
            with open('credentials', 'r') as f:
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
        resp = getattr(rq, method)(url, **kwargs)
        if resp.status_code == 200:
            return json.loads(resp.text)
        else:
            print(resp)

    def get(self, url, **data):
        import urllib.parse as urlparse
        parsed = urlparse.urlparse(url)
        data = {key: value[0] for key, value in urlparse.parse_qs(parsed.query).items()}
        url = url[:url.find('?')]
        return self._request(url, 'get', params=data)

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


@alyx.command()
@click.argument('path')
@click.pass_context
def get(ctx, path):
    client = ctx.obj['client']
    click.echo(get_table(client.get(path)))


if __name__ == '__main__':
    alyx(obj={})
