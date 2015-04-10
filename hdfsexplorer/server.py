import json
import re
import subprocess

import bottle
from bottle import abort, request, response, SimpleTemplate


API_VERSION = '/v1'
DEFAULT_LIMIT = 25
MAX_LIMIT = 100
DEFAULT_LINES = 10
MAX_LINES = 200


class Bottle(bottle.Bottle):

    def default_error_handler(self, res):
        """Return all error messages in JSON"""

        res.content_type = 'application/json'
        return json.dumps({'status': res.status, 'error': res.body})


class ResponseHeadersPlugin(object):
    """Bottle plugin to set default response headers."""

    name = 'default_response_headers'
    api = '2'

    def apply(self, callback, route):
        def wrapper(*args, **kwargs):
            body = callback(*args, **kwargs)
            return body
        return wrapper

app = Bottle()
app.install(ResponseHeadersPlugin())


@app.get(API_VERSION + '/files')
def files_root():
    return files("")


@app.get(API_VERSION + '/files<path:path>')
def files(path):
    params = _parse_and_validate_parameters(request)

    # files = list_files(path)
    all_files = recursive_list_files(path)
    files = parse_file_tree(all_files)

    if len(files) == 0:
        abort(404, 'No such folder %s' % path)

    if len(files) == 1:
        if not files[0]['is_dir']:
            return render_file(files[0], params)

    if '*' in path:
        base_dir = path[:path.index('/*')]
    else:
        base_dir = path

    files.extend(dir_details(base_dir))

    return render_dir(path, files, params, response)


def render_file(file_details, params):
    # TODO identify type of file and change MIME type appropriately
    response.content_type = 'application/json'
    return cat_hdfs_file(file_details['path'],
                         params['limit'],
                         params['offset'])


# TODO refactor so not so many args
def render_dir(path, files, params, response):
    response.content_type = 'text/html'
    tp = SimpleTemplate(name='templates/list_files.tpl')
    return tp.render(path=path,
                     files=files)


# TODO rename
def dir_details(base_dir):
    base_dir_details = {
        'perms': None,  # eventually can fetch this from cache?
        'replicas': '-',
        'user': None,  # eventually can fetch this from cache?
        'group': None,  # eventually can fetch this from cache?
        'size': '0',
        'mod_date': '2015-04-09',  # should fix this
        'mod_time': '00:00',  # should fix this
        'path': base_dir,
        'name': '.',
        'is_dir': True,
    }

    if base_dir == '/':
        return [base_dir_details]

    parent_dir = base_dir[:base_dir.rfind('/')]
    if not parent_dir:
        parent_dir = '/'

    parent_dir_details = {
        'perms': None,  # eventually can fetch this from cache?
        'replicas': '-',
        'user': None,  # eventually can fetch this from cache?
        'group': None,  # eventually can fetch this from cache?
        'size': '0',
        'mod_date': '2015-04-09',  # should fix this
        'mod_time': '00:00',  # should fix this
        'path': parent_dir,
        'name': '..',
        'is_dir': True,
    }

    return [base_dir_details, parent_dir_details]


def list_files(path):
    ls = subprocess.Popen(['hadoop', 'fs', '-ls', path],
                          stdout=subprocess.PIPE)

    lines = [line for line in ls.stdout if relevant(path, line)]
    return map(parse_file_details, lines)


def recursive_list_files(path):
    ls = subprocess.Popen(['hadoop', 'fs', '-ls', '-R', path],
                          stdout=subprocess.PIPE)

    lines = [line for line in ls.stdout if relevant(path, line)]
    return map(parse_file_details, lines)


def parse_file_tree(files):
    # yup, this is hideous
    # TODO rewrite this whole method to be more efficient
    children = []
    for f in files:
        for ff in files:
            if f['path'] == ff['path']:
                continue
            if f['path'].startswith(ff['path']):
                children.append(f['path'])
                if 'children' not in ff:
                    ff['children'] = [f]
                else:
                    ff['children'].append(f)

    # iterate again, 'cause reasons
    # (filtering out non-direct children
    # e.g. given [a, a/b, a/b/c]
    # we now have a -> a/b, a -> a/b/c, a/b -> a/b/c
    # We want a-> a/b, a/b -> a/b/c )
    top_level = []
    for f in files:
        if f['path'] not in children:
            top_level.append(f)
        if 'children' not in f:
            continue
        f['children'] = [ch for ch in f['children']
                         if ch['path'].replace(f['path'], '').rfind('/') < 1]

    return top_level


def relevant(path, line):
    pattern = path.replace('*', '[^/]*')
    return re.search(pattern, line)


def parse_file_details(line):
    parts = line.split()
    details = {
        'perms': parts[0],
        'replicas': parts[1],
        'user': parts[2],
        'group': parts[3],
        'size': parts[4],
        'mod_date': parts[5],
        'mod_time': parts[6],
        'path': parts[7],
        'name': name_from_path(parts[7])
    }
    details['is_dir'] = details['replicas'] == '-'

    return details


def name_from_path(path):
    return path[path.rfind('/') + 1:]


def cat_hdfs_file(filename, limit, offset):
    # This is quite 'novel'. Can it be done better?
    cat = subprocess.Popen(['hadoop', 'fs', '-cat', filename],
                           stdout=subprocess.PIPE)

    # 1 extra line to account for log warning. THIS IS A HACK
    head = subprocess.Popen(['head', '-n' + str(limit + offset + 1)],
                            stdin=cat.stdout,
                            stdout=subprocess.PIPE)

    tail = subprocess.Popen(['tail', '-n' + str(limit)],
                            stdin=head.stdout,
                            stdout=subprocess.PIPE)

    return tail.stdout


@app.get(API_VERSION + '/_ping')
def ping():
    """Ping endpoint to report the service is operational.

    Args:
    Returns:
        String with success message in JSON format.
    """

    return _ping()


def _ping():
    return json.dumps({'message': 'pong'})


def _parse_and_validate_parameters(request):
    parameters = {}

    # limit parameter
    try:
        limit = int(request.query.limit or DEFAULT_LIMIT)
    except:
        abort(400, 'Invalid limit parameter')
    if limit <= 0:
        abort(400, 'Invalid parameter: limit must be > 0')
    if limit > MAX_LIMIT:
        abort(400, 'Limit exceeds maximum of %d' % MAX_LIMIT)
    parameters['limit'] = limit

    # offset parameter
    try:
        offset = int(request.query.offset or 0)
    except:
        abort(400, 'Invalid offset parameter')
    if offset < 0:
        abort(400, 'Invalid parameter: offset must be > 0')
    parameters['offset'] = offset

    return parameters
