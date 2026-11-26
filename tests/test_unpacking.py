"""Tests for the ceiling on what an answer may hold once unpacked
(feed_common.MAX_UNPACKED, 200 MB): gzip and deflate in transit, the zips of
ENTSO-E and OMIE's zip of a year (a real one, tests/fixtures/price_history).
The ceiling is lowered for the tests, so that no test has to make 200 MB.

Run from the repository root:  python -m unittest discover -s tests -v
"""

import contextlib
import gzip
import io
import os
import unittest
import zipfile
import zlib
from unittest import mock

import feed_common as fc
import fetch_open_prices as op
import fetch_price_history as ph

OMIE_2018 = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'price_history',
                         'omie_marginalpdbc_2018.zip')
MB = 1024 * 1024


class Answer:
    """What urllib.request.urlopen gives, for http_get."""

    def __init__(self, body, headers):
        self.body, self.headers, self.status = io.BytesIO(body), headers, 200

    def read1(self, size):
        return self.body.read(size)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class Unpacking(unittest.TestCase):

    def test_gzip_and_deflate_as_before(self):
        body = b'<xml>' + b'1234567890' * 1000 + b'</xml>'
        self.assertEqual(fc.unpack(gzip.compress(body), 16 + zlib.MAX_WBITS), body)
        self.assertEqual(fc.unpack(gzip.compress(body) + gzip.compress(b'more'), 16 + zlib.MAX_WBITS), body + b'more')
        self.assertEqual(fc._decode(zlib.compress(body), {'content-encoding': 'deflate'}), body)
        raw = zlib.compressobj(wbits=-zlib.MAX_WBITS)
        self.assertEqual(fc._decode(raw.compress(body) + raw.flush(), {'content-encoding': 'deflate'}), body)

    def test_an_answer_past_the_ceiling_is_refused_with_a_clear_error(self):
        bomb = gzip.compress(b'\0' * (2 * MB))
        with mock.patch.object(fc, 'MAX_UNPACKED', MB):
            with self.assertRaises(fc.TooLarge):
                fc.unpack(bomb, 16 + zlib.MAX_WBITS)
            with mock.patch('urllib.request.urlopen', return_value=Answer(bomb, {'Content-Encoding': 'gzip'})):
                response = fc.http_get('https://example.org/x')
        self.assertIsNone(response.status)
        self.assertEqual(response.error, 'more than 1 MB once unpacked: refused')

    def test_a_zip_of_entsoe_past_the_ceiling_is_refused(self):
        packed = io.BytesIO()
        with zipfile.ZipFile(packed, 'w', zipfile.ZIP_DEFLATED) as archive:
            archive.writestr('a.xml', b'\0' * (2 * MB))
        with mock.patch.object(fc, 'MAX_UNPACKED', MB):
            answer = fc.entsoe_answer(fc.Response(200, {}, packed.getvalue()))
        self.assertEqual((answer.documents, answer.problem), ([], 'a zip of more than 1 MB once unpacked: refused'))
        self.assertEqual(len(fc.entsoe_answer(fc.Response(200, {}, packed.getvalue())).documents), 1)

    def test_omie_s_zip_of_a_year_past_the_ceiling_is_refused(self):
        with open(OMIE_2018, 'rb') as f:
            body = f.read()
        get = lambda url, timeout=120: fc.Response(200, {}, body)   # noqa: E731
        with mock.patch.object(fc, 'MAX_UNPACKED', 100 * 1024), self.assertRaises(op.SourceDown) as refused:
            ph.omie_year_zip(2018, get)
        self.assertIn('more than 0 MB once unpacked', str(refused.exception))
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertGreater(len(ph.omie_year_zip(2018, get)), 700)      # within the ceiling, as before


if __name__ == '__main__':
    unittest.main()
