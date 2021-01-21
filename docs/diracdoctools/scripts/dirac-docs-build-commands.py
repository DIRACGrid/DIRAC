#!/usr/bin/env python
"""dirac-docs-build-commands.py

  Build scripts documentation from the scripts docstrings. The scripts are not
  very uniform

"""
import sys

from diracdoctools.diraccmd.commandReference import run
from diracdoctools.Config import CLParser
sys.exit(run(**(CLParser().optionDict())))
