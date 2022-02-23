"""
Just a selector for X509Certificate
"""
from pkgutil import extend_path


#####
# SUPER DISGUSTING HACK
# We define these variables, and then remove them immediately.
# it is to allow something like 'from DIRAC.Core.Security import X509Chain'
# But pylint would complain just like that
# I've spent a lot of time trying to get pylint to work, but...
# https://github.com/PyCQA/pylint/issues/2474

X509Chain = None
X509CRL = None
X509Certificate = None
X509Request = None

locals().pop("X509Chain")
locals().pop("X509CRL")
locals().pop("X509Certificate")
locals().pop("X509Request")
####


# Add the m2crypto subpackage to the search path
# This allows imports like 'from DIRAC.Core.Security.X509Chian...' to work transparently
# Nice kind of tricks you find in libraries like xml...
__path__ = extend_path(__path__, __name__ + ".m2crypto")
