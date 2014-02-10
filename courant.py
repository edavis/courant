#!/usr/bin/env python

import os
import json
import urlparse
from cStringIO import StringIO
from xml.etree import ElementTree
from collections import defaultdict

import boto
import arrow
import requests
import redis as redis_pkg
from flask import Flask, request, redirect
from boto.s3.bucket import Bucket
from boto.s3.key import Key


app = Flask(__name__)
redis_url = os.getenv('REDISTOGO_URL', 'redis://localhost:6379')
redis = redis_pkg.from_url(redis_url)

s3_conn = boto.connect_s3()
s3_bucket = s3_conn.lookup('dir.rsshub.org')

VERSION = '0.1'
PACK_MAGIC_PATTERN = '<[{~#--- '


def build_timestamp():
    """
    Return current UTC as a string.

    :return: Current UTC timestamp
    :rtype: str

    >>> build_timestamp()
    'Sun Feb 09 2014 22:57:01 -0000'
    """
    now = arrow.utcnow()
    return now.format('ddd MMM DD YYYY HH:mm:ss Z')


def build_jsonp(obj, func='getData'):
    """
    Return a JSONP string with obj serialized and padded with func.

    :param dict obj: Dict to be serialized
    :param str func: Name of the callback function (default 'getData')
    :return: JSONP version of object padded with func
    :rtype: str
    """
    buf = StringIO()
    buf.write('%s(' % func)
    json.dump(obj, buf, sort_keys=True)
    buf.write(')')
    return buf.getvalue()


def build_response(obj):
    """
    Helper method to return a JSONP response.

    :param dict obj: Dict to be serialized as JSONP
    :return: Tuple of JSONP, status code, HTTP headers
    :rtype: tuple
    """
    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': 'fargo.io',
    }
    return build_jsonp(obj), 200, headers


def name_from_link(link):
    """
    Return the base name from a name URL.

    :param str link: Outline name as a URL (i.e., as returned from newOutlineName)
    :return: Outline base name
    :rtype: str

    >>> name_from_link('http://noteric.rsshub.org/')
    'noteric'
    """
    netloc = urlparse.urlparse(link).netloc
    (name, domain) = netloc.split('.', 1)
    return name


def pack_url(name):
    """
    Return the pack URL associated with a named outline.

    :param str name: Outline name
    :return: Pack URL for the named outline
    :rtype: str
    """
    opml_url = redis.hget('names:%s' % name, 'opmlUrl')
    resp = requests.get(opml_url)
    resp.raise_for_status()
    doc = ElementTree.fromstring(resp.content)
    return doc.findtext('head/linkHosting')


def handle_pack_file(name):
    """
    Parse pack file and upload files to S3.

    :param str name: Outline name
    """
    url = pack_url(name)
    resp = requests.get(url)
    resp.raise_for_status()

    paths = defaultdict(list)
    current_path = None
    for line in resp.iter_lines():
        if line.startswith(PACK_MAGIC_PATTERN):
            offset = len(PACK_MAGIC_PATTERN)
            current_path = line[offset:].lstrip('/')
            continue
        else:
            if current_path is not None:
                paths[current_path].append(line)

    for path, content in paths.iteritems():
        full_path = '%s/%s' % (name, path)
        key = Key(s3_bucket, full_path)
        key.set_metadata('Content-Type', 'text/html')
        key.set_contents_from_string('\n'.join(content), policy='public-read')


@app.route('/pingPackage')
def ping_package():
    """
    Upload pack file to S3 and update named outline metadata.
    """
    link = request.args.get('link', '') # ex: http://noteric.rsshub.org/
    name = name_from_link(link)
    handle_pack_file(name)
    obj = {
        'whenLastUpdate': build_timestamp(),
        'urlRedirect': 'http://dir.rsshub.org/%s/' % name,
        'ctUpdates': redis.incr('counter:%s' % name),
    }
    redis.hmset('names:%s' % name, obj)
    return build_response({
        'url': link,
    })


@app.route('/isNameAvailable')
def name_available():
    """
    Display whether the given name has already been taken.
    """
    name = request.args.get('name', '')
    if not name:
        return build_response({'message': ''})
    elif len(name) < 4:
        return build_response({'message': 'Name must be 4 or more characters.'})

    name_exists = redis.exists('names:%s' % name)
    if name_exists:
        msg = '%s.rsshub.org is not available.' % name
    else:
        msg = '%s.rsshub.org is available.' % name
    return build_response({'message': msg, 'available': not name_exists})


@app.route('/newOutlineName')
def new_outline():
    """
    Associate a name with an outline.
    """
    name = request.args.get('name', '')
    url = request.args.get('url', '')
    if not url:
        return build_response({
            'flError': True,
            'errorString': "Can't assign the name because there is no url parameter provided.",
        })
    if redis.exists('names:%s' % name):
        return build_response({
            'flError': True,
            'errorString': "Can't assign the name '%s' to the outline because there already is an outline with that name." % name,
        })

    obj = {
        'name': name,
        'opmlUrl': url,
        'whenCreated': build_timestamp(),
    }
    redis.hmset('names:%s' % name, obj)
    return build_response({
        'flError': False,
        'name': '%s.rsshub.org' % name,
    })


@app.route('/getUrlFromName')
def url_from_name():
    """
    Return the OPML URL associated with a given name.
    """
    name = request.args.get('name', '')
    if not redis.exists('names:%s' % name):
        return build_response({
            'flError': True,
            'errorString': "Can't open the outline named '%s' because there is no outline with that name." % name,
        })

    opml_url = redis.hget('names:%s' % name, 'opmlUrl')
    assert opml_url is not None, "no opmlUrl found for '%s'" % name
    return build_response({
        'flError': False,
        'url': opml_url,
    })


@app.route('/names/<name>')
def display_name(name):
    """
    Display a name record.

    :param str name: Outline name
    """
    obj = redis.hgetall('names:%s' % name)
    if 'ctUpdates' in obj:
        # Redis stores everything as strings, so coerce ctUpdates
        # right before sending.
        obj['ctUpdates'] = int(obj['ctUpdates'])
    return json.dumps(obj, sort_keys=True), 200, {'Content-Type': 'application/json'}


@app.route('/version')
def version():
    """
    Display the version of the app in plain text.
    """
    return VERSION, 200, {'Content-Type': 'text/plain'}


redirect_app = Flask(__name__)
@redirect_app.route('/')
def redirect_name():
    """
    Take a full name URL and redirect to its stored files.
    """
    host = request.headers.get('Host')
    (name, domain) = host.split('.', 1)
    url = redis.hget('names:%s' % name, 'urlRedirect')
    return redirect(url)

class Dispatch(object):
    def __call__(self, environ, start_response):
        active_app = app if environ['HTTP_HOST'] == 'pub.rsshub.org' else redirect_app
        return active_app(environ, start_response)

application = Dispatch()

if __name__ == '__main__':
    app.run(debug=True)
    # redirect_app.run(debug=True)
