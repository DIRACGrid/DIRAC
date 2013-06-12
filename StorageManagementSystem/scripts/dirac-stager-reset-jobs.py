#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-stager-reset-jobs
# Author :  Daniela Remenska
########################################################################
"""
  Reset any file staging requests that belong to particular job(s)
"""
_RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC                                     import gConfig, gLogger, exit as DIRACExit, S_OK, version