#!/usr/bin/env python
"""
See https://github.com/DIRACGrid/management/blob/master/dirac-install.py
"""
import os
import stat
import sys
import tempfile
try:
  # For Python 3.0 and later
  from urllib.request import urlopen
except ImportError:
  # Fall back to Python 2's urllib2
  from urllib2 import urlopen

# Download dirac-install.py
response = urlopen(
    "https://raw.githubusercontent.com/DIRACGrid/management/master/dirac-install.py"
)
code = response.getcode()
if code > 200 or code >= 300:
  raise RuntimeError("Failed to download dirac-install.py with code %s" % code)

# Write dirac-install.py to a temporay file
tmpHandle, tmp = tempfile.mkstemp()
fp = os.fdopen(tmpHandle, "wb")
fp.write(response.read())
fp.close()

# Make the dirac-install.py temporary file executable
st = os.stat(tmp)
os.chmod(tmp, st.st_mode | stat.S_IEXEC)

# Suppress Python warnings in dirac-install.py
os.environ["PYTHONWARNINGS"] = "ignore"

# Replace the current process with the actual dirac-install.py script
os.execv(tmp, sys.argv)
