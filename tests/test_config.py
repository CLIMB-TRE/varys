"""Configuration parsing tests. Requires no broker.

Moved out of test_varys.py so the broker / broker-free split matches module
boundaries; `pytest -m "not broker"` collects this module.
"""

import json
import os
import tempfile
import unittest

from varys import Varys

DIR = os.path.dirname(__file__)
LOG_FILENAME = os.path.join(DIR, "test.log")


class TestVarysConfig(unittest.TestCase):
    def setUp(self):
        handle, self.config_path = tempfile.mkstemp()
        os.close(handle)

    def tearDown(self):
        os.remove(self.config_path)

    def test_config_not_json(self):
        with open(self.config_path, "w") as f:
            f.write("asdf9υ021ζ3;-ö×=()[]{}∇Δοo")

        # use a context manager so we can check SystemExit code
        with self.assertRaises(SystemExit) as cm:
            Varys("test", LOG_FILENAME, config_path=self.config_path)

        self.assertEqual(cm.exception.code, 11)

    def test_config_profile_missing(self):
        config = {
            "version": "0.2",  # bad version prints warning but doesn't raise error
            "profiles": {"asdfadsf": {}},
        }

        with open(self.config_path, "w") as f:
            json.dump(config, f, ensure_ascii=False)

        with self.assertRaises(SystemExit) as cm:
            Varys("test", LOG_FILENAME, config_path=self.config_path)

        self.assertEqual(cm.exception.code, 2)

    def test_config_profile_incomplete(self):
        config = {
            "version": "0.1",
            "profiles": {
                "test": {
                    "username": "username",
                    "extra": "unnecessary",
                }
            },
        }

        with open(self.config_path, "w") as f:
            json.dump(config, f, ensure_ascii=False)

        with self.assertRaises(SystemExit) as cm:
            Varys("test", LOG_FILENAME, config_path=self.config_path)

        self.assertEqual(cm.exception.code, 11)


if __name__ == "__main__":
    unittest.main()
