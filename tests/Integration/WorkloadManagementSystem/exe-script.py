#!/usr/bin/env python
"""Script to run Executable application"""

import shlex
import subprocess
import sys

# Main
if __name__ == "__main__":
    sys.exit(subprocess.call(shlex.split("echo 'Hello World'")))
