import json
import requests as rq
import os
import os.path as op
import sys

"""
tool ls --session=...  # list datasets
        --status=pending/uploading/done
    <dataset>    date    ...    file_record_1  file_record_2   status
tool upload --status=pending
"""


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


def get_token():
    path = get_token_path()
    if not op.exists(path):
        return
    with open(path, 'r') as f:
        return json.load(f).get('token', None)


def is_authenticated():
    return True if get_token() else False


def make_end_point(path=''):
    if path.startswith('/'):
        path = path[1:]
    return BASE_URL + path


def _request(url, method, **kwargs):
    token = get_token()
    if token:
        kwargs['headers'] = {'Authorization': 'Token ' + token}
    resp = getattr(rq, method)(url, **kwargs)
    if resp.status_code == 200:
        return json.loads(resp.text)


def get(url):
    return _request(url, 'get')


def post(url, **data):
    return _request(url, 'post', data=data)


def put(url, **data):
    return _request(url, 'put', data=data)


def patch(url, **data):
    return _request(url, 'patch', data=data)


def auth(username, password):
    if is_authenticated():
        return True
    url = make_end_point('/auth-token/')
    resp = post(url, username=username, password=password)
    if not resp:
        return False
    write_token(resp)
    return is_authenticated()


if __name__ == '__main__':
    if not is_authenticated():
        # Open credentials, a text file in '.' with just <username>:<password>
        with open('credentials', 'r') as f:
            username, password = f.read().strip().split(':')
        # This command saves a ~/.alyx/auth-token.json file with a token.
        auth(username, password)
    # Return the token for the local user, if the token exists in ~/.alyx.
    print(get_token())
