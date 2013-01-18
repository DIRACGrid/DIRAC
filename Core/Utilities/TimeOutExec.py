
########################################################################
# $HeadURL $
# File: TimeOutExec.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/02/17 08:39:10
########################################################################

""" :mod: TimeOutExec 
    =======================
 
    .. module: TimeOutExec
    :synopsis: decorator for time out execution
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    decorator for time out execution of a function
"""

__RCSID__ = "$Id $"

##
# @file TimeOutExec.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/02/17 08:39:25
# @brief Definition of TimeOutExec decorator.

## imports 
import signal
## from DIRAC
from DIRAC.Core.Utilities.ReturnValues import S_ERROR

def execTimeOut( timeOut ):
  """ simple execution with time out decorator

  :usage: this is decorator, you can stick it just befpre function definition 

    @execTimeOut( 100 ) 
    def myFuntion( ... )

  or alternatively, if you want to set time out dynamically 

    def myFunction( ... ):
      ...

    timeOut = ...
    myFunction = execTimeOut( timeOut )( myFunction )()
  
  :warning: it is using internally SIGALRM, make sure you're 
  not using this signal somewhere else 

  :param int timeOut: time out in seconds 
  """
  class TimedOutError( Exception ): 
    """ dummy exception raised in SIGALRM handler"""
    pass
  def timeout(f):
    def inner(*args):
      def timeOutHandler(signum, frame):
        raise TimedOutError()
      saveHandler = signal.signal(signal.SIGALRM, timeOutHandler) 
      signal.alarm(timeOut) 
      try: 
        ret = f()
      except TimedOutError:
        ret = S_ERROR( "execution has timed out after %s sec" % timeOut )
      finally:
        signal.signal(signal.SIGALRM, saveHandler) 
      signal.alarm(0)
      return ret
    return inner
  return timeout



