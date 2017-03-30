#!/usr/bin/env python
'''Script to run Executable application'''

from os import system
import sys

# Main
if __name__ == '__main__':
  sys.exit(system('''echo Hello World''')/256)
