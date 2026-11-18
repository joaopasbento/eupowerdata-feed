"""Real answers of ENTSO-E for the tests of the newer fetchers, and a stand-in
for ENTSO-E that gives them back. Not a test module.

The answers are in tests/fixtures/entsoe, as ENTSO-E served them on 18 Nov
2026 at 18:07 UTC to the requests the fetchers make (the larger ones gzipped):
that is the time the tests run the fetchers at.
"""

import gzip
import os
import urllib.parse
from datetime import datetime, timezone

from feed_common import Response

FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures', 'entsoe')
ASKED_AT = datetime(2026, 11, 18, 18, 7, 42, tzinfo=timezone.utc)     # when the answers were obtained
KEY = 'not-a-real-key'


def sample(name):
    """A kept answer, as bytes."""
    path = os.path.join(FIXTURES, name)
    if os.path.exists(path + '.gz'):
        with gzip.open(path + '.gz', 'rb') as f:
            return f.read()
    with open(path, 'rb') as f:
        return f.read()


class Entsoe:
    """ENTSO-E for a test. `answers` is a list of (what the request must have,
    what comes back): the first whose parameters all match is used. What comes
    back is the name of a kept answer, bytes, a Response, or None for a
    request that gets no answer. A request that matches nothing is answered
    "no matching data". `asked` has the parameters of every request, without
    the key."""

    def __init__(self, answers=()):
        self.answers = list(answers)
        self.asked = []

    def __call__(self, url, timeout=None, max_seconds=None, headers=None):
        params = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
        assert params.pop('securityToken', None) == KEY, 'the request did not carry the API key'
        self.asked.append(params)
        for wanted, answer in self.answers:
            if all(params.get(name) == value for name, value in wanted.items()):
                if answer is None:
                    return Response(error='timed out')
                if isinstance(answer, Response):
                    return answer
                return Response(200, {}, sample(answer) if isinstance(answer, str) else answer)
        return Response(200, {}, sample('no_data.xml'))

    def asked_for(self, **wanted):
        """The requests that had these parameters."""
        return [params for params in self.asked if all(params.get(k) == v for k, v in wanted.items())]
