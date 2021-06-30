#!/usr/bin/env python
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

sys.stderr.write("#" * 100 + "\n")
sys.stderr.write("#" * 100 + "\n")
sys.stderr.write("#" * 100 + "\n")
sys.stderr.write("#" * 100 + "\n")
sys.stderr.write("#" * 100 + "\n")
sys.stderr.write("\n")
sys.stderr.write("Getting dirac-install from this location is no longer supported!\n")
sys.stderr.write("\n")
sys.stderr.write("Please update your scripts to use:\n")
sys.stderr.write("    https://raw.githubusercontent.com/DIRACGrid/management/master/dirac-install.py\n")
sys.stderr.write("\n")
sys.stderr.write("#" * 100 + "\n")
sys.stderr.write("#" * 100 + "\n")
sys.stderr.write("#" * 100 + "\n")
sys.stderr.write("#" * 100 + "\n")
sys.stderr.write("#" * 100 + "\n")

if os.environ.get("DIRAC_DEPRECATED_FAIL", "No").lower() in ("y", "yes", "t", "true", "on", "1"):
  raise RuntimeError("Failing as DIRAC_DEPRECATED_FAIL is set")

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

# Make the dirac-install.py temporay file executable
st = os.stat(tmp)
os.chmod(tmp, st.st_mode | stat.S_IEXEC)

# Replace the current process with the actual dirac-install.py script
os.execv(tmp, sys.argv)
