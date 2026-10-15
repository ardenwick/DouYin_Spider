import functools
import json
from os import path
from datetime import datetime
from threading import current_thread
from types import SimpleNamespace
from urllib.parse import urlparse
from utils import http_client as requests
from loguru import logger

from google.protobuf import json_format
from google.protobuf.message import Message

from .renderer import *
from .compression import *
from .message_visitor import *


def json_loads_object(s: str | bytes | bytearray):
    return json.loads(s, object_hook=lambda d: SimpleNamespace(**d))


def json_dumps_compact(o: object, **kwargs):
    kwargs.pop('separators', None)
    return json.dumps(o, ensure_ascii=False, separators=(',', ':'), **kwargs)


def format_readable_time(ts: int, mili=False):
    if isinstance(ts, float):
        ts *= 1000
        mili = True
    ts = int(ts)
    if mili:
        return datetime.fromtimestamp(ts / 1000).strftime(f'%Y-%m-%d %H:%M:%S.{ts%1000:03}')
    else:
        return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


def find_if(lst, pred: Callable) -> Any | None:
    for n in lst:
        if pred(n):
            return n


def parse_readable_time(ts: str):
    return datetime.strptime(ts.split('.', 1)[0], '%Y-%m-%d %H:%M:%S').timestamp()


def message_to_json(m: Message):
    return json_format.MessageToJson(m, preserving_proto_field_name=True, ensure_ascii=False, indent=None)


def name_thread(fn, prefix='fn-'):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        name = current_thread().name
        current_thread().name = f"{prefix}{fn.__name__}"
        try:
            return fn(*args, **kwargs)
        finally:
            current_thread().name = name
    return wrapper


def print_exception(fn, printter=logger.exception):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            printter(str(e))
            raise
    return wrapper


def download_file(root, url, domain_levels=2, request=requests.get):
    parsed = urlparse(url)

    host = parsed.hostname or 'unknown_host'
    host = '.'.join(host.split('.')[-domain_levels:])

    parts = parsed.path.lstrip('/').split('/')

    if not parts or parts[-1] == '':
        parts.append('download')

    dest_dir, filename = path.join(root, host, *parts[:-1]), parts[-1]
    dest = path.join(dest_dir, filename)

    if path.exists(dest):
        return dest

    os.makedirs(dest_dir, exist_ok=True)

    resp = request(url, stream=True)
    resp.raise_for_status()

    with open(dest, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    return dest
