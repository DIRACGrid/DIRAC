# FIXME: to bring back to life

"""  This program tests that the Logging DB can be actually queried from DIRAC
"""
import DIRAC
from DIRAC.FrameworkSystem.DB.SystemLoggingDB import SystemLoggingDB

DBpoint=SystemLoggingDB()


DIRAC.exit()
