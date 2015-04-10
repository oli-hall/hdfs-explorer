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

    files = list_files(path)
    if len(files) == 0:
        abort(404, 'No such folder %s' % path)

    if len(files) == 1:
        if not files[0]['is_dir']:
            return render_file(files[0], params)

    if '*' in path:
        base_dir = path[:path.index('/*')]
    else:
        base_dir = path

    for file in files:
        file['name'] = name_from_path(file['path'])

    files.extend(dir_details(base_dir))

    return render_dir(path, files, params, response)


def render_file(file_details, params):
    # TODO identify type of file and change MIME type appropriately
    response.content_type = 'application/json'
    return cat_hdfs_file(file_details['path'], params['lines'])


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

    # parent_dir_details = {
    #     'perms': None,  # eventually can fetch this from cache?
    #     'replicas': '-',
    #     'user': None,  # eventually can fetch this from cache?
    #     'group': None,  # eventually can fetch this from cache?
    #     'size': '0',
    #     'mod_date': '2015-04-09',  # should fix this
    #     'mod_time': '00:00',  # should fix this
    #     'path': base_dir,
    #     'name': '..',
    #     'is_dir': True,
    # }

    # return [base_dir_details, parent_dir_details]
    return [base_dir_details]


def name_from_path(path):
    return path[path.rfind('/') + 1:]


def list_files(path):
    ls = subprocess.Popen(['hadoop', 'fs', '-ls', path],
                          stdout=subprocess.PIPE)

    lines = [line for line in ls.stdout if relevant(path, line)]
    return map(parse_file_details, lines)


def relevant(path, line):
    pattern = path.replace('*', '[^/]*')
    return re.search(pattern, line)


# TODO rename this
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
        'path': parts[7]
    }
    details['is_dir'] = details['replicas'] == '-'

    return details


def cat_hdfs_file(filename, num_lines):
    cat = subprocess.Popen(['hadoop', 'fs', '-cat', filename],
                           stdout=subprocess.PIPE)
    count = 0
    output = []
    for line in cat.stdout:
        if count <= num_lines:
            count += 1
            output.append(line)

    # hack to trim hadoop warning from output
    return output[1:]

# TODO re-add support for JSON by looking at Headers
# @app.get(API_VERSION + '/files<path:path>.json')
# def files_json(path):
#     response.content_type = 'application/json'
#     return json.dumps({
#         'files': _files_for_path(path)
#     })


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

    # path parameter
    try:
        lines = int(request.query.lines or DEFAULT_LINES)
    except:
        abort(400, 'Invalid lines parameter')
    if lines <= 0:
        abort(400, 'Invalid parameter: lines must be > 0')
    if lines > MAX_LINES:
        abort(400, 'Limit exceeds maximum of %d' % MAX_LINES)
    parameters['lines'] = lines

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
