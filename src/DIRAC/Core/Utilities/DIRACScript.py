from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Utilities.Decorators import deprecated

# TODO: remove it in 8.1
@deprecated("DIRACScript is deprecated, use 'from DIRAC.Core.Base.Script import Script' instead.")
class DIRACScript(Script):
    pass
