#!/usr/bin/env python
# -*-coding:utf-8-*-
# Author: xujianfeng_sx@qiyi.com
# Time  : 2016-7-27


import time
import logging
import StringIO
from six.moves.urllib.parse import quote as _quote, unquote
from six.moves.urllib.parse import urlparse, urlunparse
import six
import requests

try:
    import simplejson as json
except ImportError:
    import json

from swiftclient.exceptions import ClientException
from swiftclient.client import HTTPConnection
from ringinfoclient.iqiyiring import iQiYiRing


MAX_OFFSET = (16 ** 16) - 1
NORMAL_FORMAT = "%016.05f"
INTERNAL_FORMAT = NORMAL_FORMAT + '_%016x'


class Timestamp(object):
    """
    Internal Representation of Swift Time.

    The normalized form of the X-Timestamp header looks like a float
    with a fixed width to ensure stable string sorting - normalized
    timestamps look like "1402464677.04188"

    To support overwrites of existing data without modifying the original
    timestamp but still maintain consistency a second internal offset vector
    is append to the normalized timestamp form which compares and sorts
    greater than the fixed width float format but less than a newer timestamp.
    The internalized format of timestamps looks like
    "1402464677.04188_0000000000000000" - the portion after the underscore is
    the offset and is a formatted hexadecimal integer.

    The internalized form is not exposed to clients in responses from
    Swift.  Normal client operations will not create a timestamp with an
    offset.

    The Timestamp class in common.utils supports internalized and
    normalized formatting of timestamps and also comparison of timestamp
    values.  When the offset value of a Timestamp is 0 - it's considered
    insignificant and need not be represented in the string format; to
    support backwards compatibility during a Swift upgrade the
    internalized and normalized form of a Timestamp with an
    insignificant offset are identical.  When a timestamp includes an
    offset it will always be represented in the internalized form, but
    is still excluded from the normalized form.  Timestamps with an
    equivalent timestamp portion (the float part) will compare and order
    by their offset.  Timestamps with a greater timestamp portion will
    always compare and order greater than a Timestamp with a lesser
    timestamp regardless of it's offset.  String comparison and ordering
    is guaranteed for the internalized string format, and is backwards
    compatible for normalized timestamps which do not include an offset.
    """

    def __init__(self, timestamp, offset=0):
        if isinstance(timestamp, basestring):
            parts = timestamp.split('_', 1)
            self.timestamp = float(parts.pop(0))
            if parts:
                self.offset = int(parts[0], 16)
            else:
                self.offset = 0
        else:
            self.timestamp = float(timestamp)
            self.offset = getattr(timestamp, 'offset', 0)
        # increment offset
        if offset >= 0:
            self.offset += offset
        else:
            raise ValueError('offset must be non-negative')
        if self.offset > MAX_OFFSET:
            raise ValueError('offset must be smaller than %d' % MAX_OFFSET)

    @property
    def normal(self):
        return NORMAL_FORMAT % self.timestamp


def normalize_timestamp(timestamp):
    """
    Format a timestamp (string or numeric) into a standardized
    xxxxxxxxxx.xxxxx (10.5) format.

    Note that timestamps using values greater than or equal to November 20th,
    2286 at 17:46 UTC will use 11 digits to represent the number of
    seconds.

    :param timestamp: unix timestamp
    :returns: normalized timestamp as a string
    """
    return Timestamp(timestamp).normal


def safe_value(name, value):
    """
    Only show up to logger_settings['reveal_sensitive_prefix'] characters
    from a sensitive header.

    :param name: Header name
    :param value: Header value
    :return: Safe header value
    """
    if name.lower() in LOGGER_SENSITIVE_HEADERS:
        prefix_length = logger_settings.get('reveal_sensitive_prefix', 16)
        prefix_length = int(
            min(prefix_length, (len(value) ** 2) / 32, len(value) / 2)
        )
        redacted_value = value[0:prefix_length]
        return redacted_value + '...'
    return value


def scrub_headers(headers):
    """
    Redact header values that can contain sensitive information that
    should not be logged.

    :param headers: Either a dict or an iterable of two-element tuples
    :return: Safe dictionary of headers with sensitive information removed
    """
    if isinstance(headers, dict):
        headers = headers.items()
    headers = [
        (parse_header_string(key), parse_header_string(val))
        for (key, val) in headers
    ]
    if not logger_settings.get('redact_sensitive_headers', True):
        return dict(headers)
    if logger_settings.get('reveal_sensitive_prefix', 16) < 0:
        logger_settings['reveal_sensitive_prefix'] = 16
    return {key: safe_value(key, val) for (key, val) in headers}


try:
    from logging import NullHandler
except ImportError:
    # Added in Python 2.7
    class NullHandler(logging.Handler):
        def handle(self, record):
            pass

        def emit(self, record):
            pass

        def createLock(self):
            self.lock = None

logger = logging.getLogger("swiftclient")
logger.addHandler(NullHandler())
logger_settings = {
    'redact_sensitive_headers': True,
    'reveal_sensitive_prefix': 16
}
LOGGER_SENSITIVE_HEADERS = [
    'x-auth-token', 'x-auth-key', 'x-service-token', 'x-storage-token',
    'x-account-meta-temp-url-key', 'x-account-meta-temp-url-key-2',
    'x-container-meta-temp-url-key', 'x-container-meta-temp-url-key-2',
    'set-cookie'
]


def http_log(args, kwargs, resp, body):
    if not logger.isEnabledFor(logging.INFO):
        return

    # create and log equivalent curl command
    string_parts = ['curl -i']
    for element in args:
        if element == 'HEAD':
            string_parts.append(' -I')
        elif element in ('GET', 'POST', 'PUT'):
            string_parts.append(' -X %s' % element)
        else:
            string_parts.append(' %s' % element)
    if 'headers' in kwargs:
        headers = scrub_headers(kwargs['headers'])
        for element in headers:
            header = ' -H "%s: %s"' % (element, headers[element])
            string_parts.append(header)

    # log response as debug if good, or info if error
    if resp.status < 300:
        log_method = logger.debug
    else:
        log_method = logger.info

    log_method("REQ: %s", "".join(string_parts))
    log_method("RESP STATUS: %s %s", resp.status, resp.reason)
    log_method("RESP HEADERS: %s", scrub_headers(resp.getheaders()))
    if body:
        log_method("RESP BODY: %s", body)


def quote(value, safe='/'):
    """
    Patched version of urllib.quote that encodes utf8 strings before quoting.
    On Python 3, call directly urllib.parse.quote().
    """
    if six.PY3:
        return _quote(value, safe=safe)
    return _quote(encode_utf8(value), safe)


def encode_utf8(value):
    if isinstance(value, six.text_type):
        value = value.encode('utf8')
    return value


def parse_header_string(data):
    if not isinstance(data, (six.text_type, six.binary_type)):
        data = str(data)
    if six.PY2:
        if isinstance(data, six.text_type):
            # Under Python2 requests only returns binary_type, but if we get
            # some stray text_type input, this should prevent unquote from
            # interpreting %-encoded data as raw code-points.
            data = data.encode('utf8')
        try:
            unquoted = unquote(data).decode('utf8')
        except UnicodeDecodeError:
            try:
                return data.decode('utf8')
            except UnicodeDecodeError:
                return quote(data).decode('utf8')
    else:
        if isinstance(data, six.binary_type):
            # Under Python3 requests only returns text_type and tosses (!) the
            # rest of the headers. If that ever changes, this should be a sane
            # approach.
            try:
                data = data.decode('ascii')
            except UnicodeDecodeError:
                data = quote(data)
        try:
            unquoted = unquote(data, errors='strict')
        except UnicodeDecodeError:
            return data
    return unquoted


def resp_header_dict(resp):
    resp_headers = {}
    for header, value in resp.getheaders():
        header = parse_header_string(header).lower()
        resp_headers[header] = parse_header_string(value)
    return resp_headers


def store_response(resp, response_dict):
    """
    store information about an operation into a dict

    :param resp: an http response object containing the response
                 headers
    :param response_dict: a dict into which are placed the
       status, reason and a dict of lower-cased headers
    """
    if response_dict is not None:
        response_dict['status'] = resp.status
        response_dict['reason'] = resp.reason
        response_dict['headers'] = resp_header_dict(resp)


class RingManagement(object):
    """
    Manage Object/Container/Account Ring to achieve put Object/Container
    """

    def __init__(self, url='127.0.0.1'):
        self.object_ring = None
        self.container_ring = None
        self.account_ring = None
        self.object_ring_name = None
        self.container_ring_name = None
        self.account_ring_name = None
        self.url = url
        self.upload_replica_num = 1
        self.get_ring_data_from_server()

    def get_ring_data_from_server(self):
        self.url = 'http://' + self.url + ':8080' + '/ringinfo'
        res = requests.get(self.url)
        res_json = res.json()

        self.object_ring = iQiYiRing('object', res_json['object'],
                                     res_json['swift_hash_path_prefix'],
                                     res_json['swift_hash_path_suffix'])
        self.container_ring = iQiYiRing('container', res_json['container'],
                                        res_json['swift_hash_path_prefix'],
                                        res_json['swift_hash_path_suffix'])
        # self.account_ring = IQiyiRing('account', res_json['account'],
        #                               res_json['swift_hash_path_prefix'],
        #                               res_json['swift_hash_path_suffix'])

    def put_container(self, account_name=None, container_name=None, headers=None, response_dict=None):
        """ HTTP PUT Container handler."""

        container_partition, containers = self.container_ring.get_nodes(account_name, container_name)

        statuses = []
        for i in range(self.upload_replica_num):
            container_url = 'http://%s:%d/%s/%d/%s/%s' % (containers[0]['ip'], containers[0]['port'],
                                                          containers[0]['device'], container_partition,
                                                          account_name, container_name)
            parsed = urlparse(container_url)
            path = parsed.path
            http_url = parsed.scheme + '://' + parsed.netloc
            conn = HTTPConnection(http_url)
            if headers:
                headers = dict(headers)
            else:
                headers = {}
            headers['X-Timestamp'] = normalize_timestamp(time.time())

            conn.request('PUT', path, '', headers)

            resp = conn.getresponse()
            body = resp.read()
            store_response(resp, response_dict)
            http_log(('%s%s' % (container_url.replace(parsed.path, ''), path), 'PUT',),
                     {'headers': headers}, resp, body)
            if resp.status < 200 or resp.status >= 300:
                raise ClientException.from_response(resp, 'Container PUT failed', body)
            statuses.append(resp.status)

        return statuses

    def put_object(self, account_name=None, container_name=None, object_name=None,
                   uploadfile_path=None, headers=None, response_dict=None):
        """HTTP PUT Object handler.
        First to fetch container info to update the header,
        or you will not find object info in container
        even you put a container by manual"""

        container_partition, containers = self.container_ring.get_nodes(account_name, container_name)
        container_info = {'X-Container-Host': str(containers[0]['ip']) + ':' + str(containers[0]['port']),
                          'X-Container-Device': containers[0]['device'],
                          'X-Container-Partition': container_partition}

        part, nodes = self.object_ring.get_nodes(account_name, container_name, object_name)

        # there may be incompatibility problem, or just use file read
        file_data = StringIO.StringIO()
        with open(uploadfile_path, 'r') as upload_file:
            file_data.write(upload_file.read())

        # not concurrent http_connect
        statuses = []
        for i in range(self.upload_replica_num):
            object_url = 'http://%s:%d/%s/%d/%s/%s/%s' % (nodes[i]['ip'], nodes[i]['port'],
                                                          nodes[i]['device'], part,
                                                          account_name, container_name, object_name)
            parsed = urlparse(object_url)
            path = parsed.path
            http_url = parsed.scheme + '://' + parsed.netloc
            conn = HTTPConnection(http_url)
            if headers:
                headers = dict(headers)
            else:
                headers = {}
            headers['x-timestamp'] = normalize_timestamp(time.time())
            headers['Content-Type'] = 'application/octet-stream'
            headers.update(container_info)

            conn.request('PUT', path, file_data.getvalue(), headers)

            resp = conn.getresponse()
            body = resp.read()
            http_log(('%s%s' % (object_url.replace(parsed.path, ''), path), 'PUT',),
                     {'headers': headers}, resp, body)
            store_response(resp, response_dict)
            if resp.status < 200 or resp.status >= 300:
                raise ClientException.from_response(resp, 'Object PUT failed', body)
            # etag = resp.getheader('etag', '').strip('"')
            statuses.append(resp.status)

        return statuses