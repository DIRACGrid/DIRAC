#!/usr/bin/env python
"""script to concatenate the dirac.cfg file's Systems sections with the content of the ConfigTemplate.cfg files."""
import sys

from diracdoctools.diraccmd.concatcfg import run
from diracdoctools.Config import CLParser
sys.exit(run(**(CLParser().optionDict())))
