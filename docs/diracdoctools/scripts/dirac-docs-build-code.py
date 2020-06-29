#!/usr/bin/env python
"""Create rst files for documentation of DIRAC source code."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import sys

from diracdoctools.cmd.codeReference import run, CLParser
sys.exit(run(**(CLParser().optionDict())))
