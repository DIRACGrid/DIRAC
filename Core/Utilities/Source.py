########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Source.py,v 1.3 2008/04/29 21:02:00 rgracian Exp $
# File :   Source.py
# Author : Ricardo Graciani
########################################################################

from DIRAC.Core.Utilities import S_OK, S_ERROR
from DIRAC.Core.Utilities import shellCall, systemCall
from DIRAC                import platformTuple

def Source( timeout, cmdTuple, inputEnv=None ):
  """
   Funtion to source configuration files in a platform dependent way and get 
   back the environment
  """
  # add appropiated extension to first element of the tuple (the command)
  envAsDict = '; python -c "import os,sys ; print >> sys.stderr, os.environ"'

  # 1.- Choose the right version of the configuration file
  if platformTuple[0] == 'Windows':
    cmdTuple[0] += '.bat'
  else:
    cmdTuple[0] += '.sh'

  # 2.- Check that it exists
  if not os.path.exists( cmdTuple[0] ):
    result = S_ERROR( 'Missing script: %s' % cmdTuple[0] )
    result['stdout'] = ''
    result['stderr'] = 'Missing script: %s' % cmdTuple[0]
    return result

  # Source it in a platform dependent way:
  # On windows the execution makes the environment to be inherit
  # On Linux or Darwin use bash and source the file.
  if platformTuple[0] == 'Windows':
    # this needs to be tested
    cmd = ' '.join(cmdTuple) + envAsDict
    ret = shellCall( timeout, [ cmd ], env = inputEnv ) 
  else:
    cmdTuple.insert(0,'source')
    cmd = ' '.join(cmdTuple) + envAsDict
    ret = systemCall( timeout, [ '/bin/bash', '-c', cmd ], env = inputEnv )

  # 3.- Now get back the result
  stdout = ''
  stderr = ''
  result = S_OK()
  if ret['OK']:
    # The Command has not timeout, retrieve stdout and stderr
    stdout = ret['Value'][1]
    stderr = ret['Value'][2] 
    if ret['Value'][0] == 0:
      # execution was OK
      try:
        result['outputEnv'] = eval( ret['Value'][2] )
      except:
        stdout = cmd + '\n' + stdout
        result = S_ERROR('Could not parse Environment dictionary from stderr')
    else:
      # execution error
      stdout = cmd + '\n' + stdout
      result = S_ERROR('Execution returns %s' % ret['Value'][0] )
  else:
    # Timeout
    stdout = cmd
    stderr = ret['Message']
    result = S_ERROR( stderr )

  # 4.- Put stdout and stderr in result structure
  result['stdout'] = stdout
  result['stderr'] = stderr
  
  return result
