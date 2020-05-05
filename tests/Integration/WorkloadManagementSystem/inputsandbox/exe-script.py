#!/usr/bin/env python
'''Script to run Executable application'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
from os import system

# Main
if __name__ == '__main__':

  sys.exit(system('''echo Hello World''')/256)
