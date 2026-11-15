"""Shared helpers for the feed's newer fetchers (standard library only).

Every request made through http_get() identifies the feed honestly: a real
User-Agent with the feed's name and https://eupowerdata.com as the contact,
no browser User-Agent and no Referer. A source that only answers to a
disguised client is reported as failing, never worked around.
"""

import gzip
import http.client
import json
import os
import tempfile
import time
import zlib
import urllib.error
import urllib.request
from datetime import datetime, timezone

USER_AGENT = 'EUPowerData-Feed/1.0 (+https://eupowerdata.com)'


class Response:
    """Outcome of http_get(): it never raises, so callers branch on .ok / .status."""

    def __init__(self, status=None, headers=None, body=b'', error=None):
        self.status = status            # HTTP status, or None when there was no answer
        self.headers = headers or {}    # header names in lower case
        self.body = body                # bytes
        self.error = error              # short reason when there is no usable body

    @property
    def ok(self):
        return self.status == 200

    def text(self, encoding='utf-8'):
        return self.body.decode(encoding, errors='replace')

    def json(self):
        return json.loads(self.body.decode('utf-8'))

    def describe(self):
        """One line for the log: what the source answered."""
        if self.status is None:
            return f'no answer ({self.error})'
        return f'HTTP {self.status}, {len(self.body)} bytes' + (f' ({self.error})' if self.error else '')


def http_get(url, timeout=30, headers=None, max_seconds=None):
    """GET `url`. `timeout` bounds connecting and each read; `max_seconds`
    (default: twice the timeout) bounds the whole request."""
    request_headers = {'User-Agent': USER_AGENT, 'Accept': '*/*', 'Accept-Encoding': 'gzip, deflate'}
    request_headers.update(headers or {})
    req = urllib.request.Request(url, headers=request_headers)
    limit = time.monotonic() + (max_seconds if max_seconds is not None else 2 * timeout)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            chunks = []
            while True:
                chunk = r.read1(65536)
                if not chunk:
                    break
                chunks.append(chunk)
                if time.monotonic() > limit:
                    return Response(error='response too slow')
            headers = _lower(r.headers)
            return Response(r.status, headers, _decode(b''.join(chunks), headers))
    except urllib.error.HTTPError as e:
        # 304, 4xx and 5xx arrive here; keep the status, the headers and the body.
        headers = _lower(e.headers)
        try:
            body = _decode(e.read(), headers)
        except Exception:  # noqa: BLE001
            body = b''
        return Response(e.code, headers, body, error=str(e.reason))
    except urllib.error.URLError as e:
        return Response(error=str(e.reason))
    except (OSError, EOFError, zlib.error, http.client.HTTPException) as e:
        return Response(error=f'{type(e).__name__}: {e}')


def _decode(body, headers):
    """Undo the Content-Encoding of a compressed answer."""
    encoding = (headers.get('content-encoding') or '').lower()
    if not body:
        return body
    if encoding == 'gzip':
        return gzip.decompress(body)
    if encoding == 'deflate':
        try:
            return zlib.decompress(body)
        except zlib.error:
            return zlib.decompress(body, -zlib.MAX_WBITS)      # raw deflate, sent by some servers
    return body


def _lower(headers):
    return {k.lower(): v for k, v in (headers.items() if headers else [])}


def now_iso():
    """Current UTC time, to the second: 2026-11-15T21:12:38+00:00."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json(path, default=None):
    """Parsed JSON file, or `default` when it is missing or unreadable."""
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def write_json(path, payload, **dump_args):
    """Write JSON through a temp file + rename, so an interrupted step can
    never leave a truncated file for the commit step to pick up."""
    dump_args.setdefault('ensure_ascii', False)
    directory = os.path.dirname(path) or '.'
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=directory, prefix='.', suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(payload, f, **dump_args)
            f.write('\n')
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
