########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Utilities/Source.py,v 1.7 2008/05/01 01:01:35 rgracian Exp $
# File :   Source.py
# Author : Ricardo Graciani
########################################################################

def Source( timeout, cmdTuple, inputEnv=None ):
  """
   Funtion to source configuration files in a platform dependent way and get 
   back the environment
  """
  import os
  import DIRAC
  # add appropiated extension to first element of the tuple (the command)
  envAsDict = '&& python -c "import os,sys ; print >> sys.stderr, os.environ"'

  # 1.- Choose the right version of the configuration file
  if DIRAC.platformTuple[0] == 'Windows':
    cmdTuple[0] += '.bat'
  else:
    cmdTuple[0] += '.sh'

  # 2.- Check that it exists
  if not os.path.exists( cmdTuple[0] ):
    result = DIRAC.S_ERROR( 'Missing script: %s' % cmdTuple[0] )
    result['stdout'] = ''
    result['stderr'] = 'Missing script: %s' % cmdTuple[0]
    return result

  # Source it in a platform dependent way:
  # On windows the execution makes the environment to be inherit
  # On Linux or Darwin use bash and source the file.
  if DIRAC.platformTuple[0] == 'Windows':
    # this needs to be tested
    cmd = ' '.join(cmdTuple) + envAsDict
    ret = DIRAC.shellCall( timeout, [ cmd ], env = inputEnv ) 
  else:
    cmdTuple.insert(0,'source')
    cmd = ' '.join(cmdTuple) + envAsDict
    ret = DIRAC.systemCall( timeout, [ '/bin/bash', '-c', cmd ], env = inputEnv )

  # 3.- Now get back the result
  stdout = ''
  stderr = ''
  result = DIRAC.S_OK()
  if ret['OK']:
    # The Command has not timeout, retrieve stdout and stderr
    stdout = ret['Value'][1]
    stderr = ret['Value'][2] 
    if ret['Value'][0] == 0:
      # execution was OK
      try:
        result['outputEnv'] = eval( stderr.split('\n')[-2]+'\n' )
        stderr = '\n'.join(stderr.split('\n')[:-2])
      except:
        stdout = cmd + '\n' + stdout
        result = DIRAC.S_ERROR('Could not parse Environment dictionary from stderr')
    else:
      # execution error
      stdout = cmd + '\n' + stdout
      result = DIRAC.S_ERROR('Execution returns %s' % ret['Value'][0] )
  else:
    # Timeout
    stdout = cmd
    stderr = ret['Message']
    result = DIRAC.S_ERROR( stderr )

  # 4.- Put stdout and stderr in result structure
  result['stdout'] = stdout
  result['stderr'] = stderr
  
  return result
