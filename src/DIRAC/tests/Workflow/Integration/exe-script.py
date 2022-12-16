#!/usr/bin/env python
"""Script to run Executable application"""
import sys
import subprocess
import shlex

# Main
if __name__ == "__main__":
    subprocess.call(shlex.split("env"))
    subprocess.call(shlex.split("echo Hello World"))
    sys.exit(0)
