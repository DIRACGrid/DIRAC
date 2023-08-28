"""Tools to build the DIRAC documentation.


Scripts to build code documentation, and also some mocking, etc.
"""
# list of packages that should be mocked in sphinx
# TODO: check if we can install more of these
DIRAC_DOC_MOCK_LIST = [
    "_arc",
    "arc",
    "cmreslogging",
    "fts3",
    "gfal2",
    "git",
    "lcg_util",
    "MySQLdb",
    "pylab",
    "pythonjsonlogger",
    "stomp",
]
