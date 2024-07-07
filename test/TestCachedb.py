import os
import shutil
import tempfile
from io import StringIO

from StandardTestFixture import StandardTestFixture

from tahoestaticfs.cachedb import json_zlib_load, json_zlib_dump
from tahoestaticfs.crypto import CryptFile


class TestJsonZlib(StandardTestFixture):

    def test_roundtrip(this):
        key = b"a"*32
        with CryptFile(this.file_name, key, 'w+b') as fp:
            for sz in [1, 2, 10, 100, 1000, 10000]:
                data = {
                    'a': ['b']*sz,
                    'b': ['c']*sz
                }

                fp.truncate(0)
                fp.seek(0)
                json_zlib_dump(data, fp)

                fp.seek(0)
                data_2 = json_zlib_load(fp)

                this.assert_equal(data_2, data)
