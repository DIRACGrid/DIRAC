#!/usr/bin/env python
"""script to concatenate the dirac.cfg file's Systems sections with the content of the ConfigTemplate.cfg files."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import sys

from diracdoctools.cmd.concatcfg import run
from diracdoctools.Config import CLParser
sys.exit(run(**(CLParser().optionDict())))
