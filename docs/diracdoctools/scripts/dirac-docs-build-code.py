#!/usr/bin/env python
"""Create rst files for documentation of DIRAC source code."""
import sys

from diracdoctools.Cmd.codeReference import run, CLParser
sys.exit(run(**(CLParser().optionDict())))
