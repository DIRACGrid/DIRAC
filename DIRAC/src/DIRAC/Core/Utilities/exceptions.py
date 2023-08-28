class DIRACInitError(RuntimeError):
    """Base exception that is raised when DIRAC initialization fails"""


class NotConfiguredError(DIRACInitError):
    """Exception that is raised when DIRAC is not configured"""


class AuthError(DIRACInitError):
    """Exception for when DIRAC initialization fails due to authentication issues"""


class DiracWarning(UserWarning):
    """Generic warning class for DIRAC"""
