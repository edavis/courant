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
from flask import Flask, request
from boto.s3.bucket import Bucket
from boto.s3.key import Key


app = Flask(__name__)
redis_url = os.getenv('REDISTOGO_URL', 'redis://localhost:6379')
redis = redis_pkg.from_url(redis_url)

s3_conn = boto.connect_s3()
s3_bucket = s3_conn.lookup('blogs.rsshub.org')

VERSION = '0.1'
PACK_MAGIC_PATTERN = '<[{~#--- '


def build_timestamp():
    """
    Return UTC now in a readable format.

    >>> build_timestamp()
    'Sun Feb 09 2014 22:57:01 -0000'
    """
    now = arrow.utcnow()
    return now.format('ddd MMM DD YYYY HH:mm:ss Z')


def build_jsonp(obj, func='getData'):
    """
    Return a JSONP string with obj passed to func.

    :param dict obj: Dict to be serialized
    :param str func: Name of the callback function (default 'getData')
    :returns: JSONP version of object padded with func
    :rtype: str
    """
    buf = StringIO()
    buf.write('%s(' % func)
    json.dump(obj, buf, sort_keys=True)
    buf.write(')')
    return buf.getvalue()


def build_response(obj):
    """
    Helper method to return a JSONP object.

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

    :param str link: The outline's full public URL
    :return: The outline's base name
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

    :param str name: Name of the outline we want the pack URL for
    :return: Pack URL for the given name
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

    :param str name: A name for an outline
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
        full_path = 'users/%s/%s' % (name, path)
        key = Key(s3_bucket, full_path)
        key.set_metadata('Content-Type', 'text/html')
        key.set_contents_from_string('\n'.join(content), policy='public-read')


@app.route('/pingpackage')
def ping_package():
    link = request.args.get('link', '') # ex: http://noteric.rsshub.org/
    name = name_from_link(link)
    handle_pack_file(name)
    obj = {
        'whenLastUpdate': build_timestamp(),
        'urlRedirect': 'http://blogs.rsshub.org/users/%s' % name,
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
    Display the name record for a given name.

    In the canonical Fargo Publisher implementation, this stuff is
    stored on S3. But because we're storing stuff in redis, have to
    make stuff visible this way.

    :param str name: Name for an outline
    """
    obj = redis.hgetall('names:%s' % name)
    if 'ctUpdates' in obj:
        obj['ctUpdates'] = int(obj['ctUpdates'])
    return json.dumps(obj, sort_keys=True), 200, {'Content-Type': 'application/json'}


@app.route('/version')
def version():
    return VERSION, 200, {'Content-Type': 'text/plain'}


if __name__ == '__main__':
    app.run(debug=True)
