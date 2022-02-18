#!/usr/bin/env python
"""Script to run Executable application"""
import sys
from os import system

# Main
if __name__ == "__main__":

    sys.exit(system("""echo Hello World""") / 256)
