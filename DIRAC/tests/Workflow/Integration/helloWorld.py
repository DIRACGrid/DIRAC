#!/usr/bin/env python
'''Script to run Executable application'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import subprocess
import shlex

# Main
if __name__ == '__main__':

  sys.exit(subprocess.call(shlex.split('echo Hello World')))
