#!/usr/bin/env python

"""Script to run Executable application"""
from os import system
import sys

# Main
if __name__ == "__main__":
    sys.exit(int(system("""cat testInputFileSingleLocation.txt""") / 256))
