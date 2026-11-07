"""Where the tests run, and therefore what Spark can do there.

* Windows without winutils: Spark cannot touch local files, so DataFrames are
  built in memory and ``localfs``/``delta`` tests are skipped.
* Linux (CI): local Spark with Delta, files under pytest's temp folders.
* Databricks serverless (the ``mdl-tests`` job): the existing Spark Connect
  session, with files in a UC Volume and Delta tables in a scratch schema
  (``MDL_TEST_SCRATCH`` and ``MDL_TEST_SCHEMA``).
"""

from __future__ import annotations

import os
import sys

ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
IS_WINDOWS = sys.platform == "win32"
FILE_IO = ON_DATABRICKS or not IS_WINDOWS or bool(os.environ.get("HADOOP_HOME"))
