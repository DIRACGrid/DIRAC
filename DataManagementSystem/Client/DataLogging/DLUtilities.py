'''
Created on May 7, 2015

@author: Corentin Berger
'''

import inspect
import os

from DIRAC.DataManagementSystem.Client.DataLogging.DLException import NoLogException
from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.Resources.Utilities import checkArgumentFormat

# different special keywords
dl_files = 'files'
dl_srcSE = 'srcSE'
dl_targetSE = 'targetSE'
dl_tuple = 'tuple'
dl_ignore_argument = 'ignore_argument'
wantedArgs = [dl_files, dl_srcSE, dl_targetSE ]

def getCallerName( skip = 3 ):
  """Get a name of a caller in the format module.class.method

     `skip` specifies how many levels of stack to skip while getting caller
     name. skip=1 means "who calls me", skip=2 "who calls my caller" etc.

     An empty string is returned if skipped levels exceed stack height
  """
  stack = inspect.stack()
  start = 0 + skip
  if len( stack ) < start + 1:
    return ''
  parentframe = stack[start][0]
  name = []
  module = inspect.getmodule( parentframe )
  if module:
    name.append( module.__name__ )

  if 'self' in parentframe.f_locals:
    # I don't know any way to detect call from the object method
    # XXX: there seems to be no way to detect static method call - it will
    #      be just a function call
    name.append( parentframe.f_locals['self'].__class__.__name__ )
  codename = parentframe.f_code.co_name
  if codename != '<module>':  # top level usually
    name.append( codename )  # function or a method
  ret = ".".join( name )
  if ret == '__main__' :
    ( filename, _lineno, _function, _code_context, _index ) = inspect.getframeinfo( parentframe )
    ret = os.path.basename( filename )
  del parentframe
  return ret

def extractArgs( argsDecorator, *args, **kwargs ):
  """ create a dict with the key and value of decorate function's arguments
      this is the default function to extract arguments
      argsDecorator is the arguments given to create the decorator
      key 'argsPosition' is needed to know which arguments is on each position
      argsPosition is a list with position of the arguments in the call of the decorate function
      ex : argsPosition = ['files','protocol','srcSE','targetSE'] if all arguments are passed in args
           argsPosition = ['files','protocol',('srcSE','sourceSE'),('targetSE','destSE')]
           this is an example if in the method srcSE and targetSE are nominal args and if theirs name are sourceSE and destSE
  """
  commonArgs = dict.fromkeys( [dl_srcSE, dl_targetSE], None )
  files = {}
  actionArgs = []
  ret = None
  extraList = []
  try :
    argsPosition = argsDecorator['argsPosition']
    i = 0
    while i < len( argsPosition ) :
      if i < len( args ):
        argName = argsPosition[i]
        if isinstance( argName, tuple ):
          # if argName is a tuple, it's because the argument is named and can be passed in kwargs
          # for example ('srcSE','sourceSE'), the first tuple's element is the name we want to get
          # and the second is the real name of the argument in the function
          argName = argName[0]
        if argName in wantedArgs:
          if argName is dl_files:
            files = checkArgumentFormat( args[i] )['Value']
            if 'valueName' in argsDecorator:
              # the value associated to each key is a string
              # valueName is the name of the value to get from dictionary
              for lfn in files:
                # creation of a new dictionary and a new list to save arguments specific to this lfn
                argDictExtra = []
                argDict = {}

                argDict['file'] = lfn

                valueName = argsDecorator['valueName']
                if valueName in wantedArgs:
                  argDict[valueName] = files[lfn]
                else:
                  argDictExtra.append( "%s = %s" % ( valueName, files[lfn] ) )

                argDict['extra'] = ','.join( argDictExtra )
                actionArgs.append( argDict )

            elif 'keysToGet' in argsDecorator:
              # the value associated to each key is a dictionary
              # keysToGet is key to take from the dictionary associated to each key
              keysToGet = argsDecorator['keysToGet']
              for lfn in files:
                # creation of a new dictionary and a new list to save arguments specific to this lfn
                argDictExtra = []
                argDict = {}

                argDict['file'] = lfn
                for keyToGet in keysToGet:

                  if keyToGet in wantedArgs:
                    argDict[keyToGet] = files[lfn].get( keysToGet[keyToGet], None )
                  else :
                    argDictExtra.append( "%s = %s" % ( keysToGet[keyToGet], files[lfn].get( keysToGet[keyToGet], None ) ) )
                argDict['extra'] = ','.join( argDictExtra )
                actionArgs.append( argDict )
            else :
              # else there is no value associated to key
              for lfn in files:
                argDict = {}
                argDict['file'] = lfn
                actionArgs.append( argDict )
          else :
            # else the argument is wanted but not dl_files
            commonArgs[argName] = args[i]
        else:
          # the argument is not wanted, we save it in extra list
          if argName is not 'self' and argName is not dl_ignore_argument:
            if args[i]:
              extraList.append( "%s = %s" % ( argName, args[i] ) )

      else :
        # argument is passed in kwargs
        argName = argsPosition[i]
        if isinstance( argName, tuple ):
          # if argname is a tuple is because the argument is named and can be passed in kwargs
          # for example ('srcSE','sourceSE'), the first tuple's element is the name we want to get
          # and the second is the real name of the argument in the function
          keyToGet = argName[1]
          argName = argName[0]
        else :
          keyToGet = argName

        if argName in wantedArgs:
          # wanted argument
          commonArgs[argName] = kwargs.pop( keyToGet, None )
        else :
          if argName is not dl_ignore_argument:
            # not wanted argument, save it in extra list
            value = kwargs.pop( argName, None )
            if value :
              extraList.append( "%s = %s" % ( argName, value ) )
      i += 1
  except Exception as e:
    gLogger.error( 'unexpected error in DLFucntions.extractArgs %s' % e )
    ret = S_ERROR( 'unexpected error in DLFucntions.extractArgs %s' % e )

  finally :
    commonArgs['extra'] = ','.join( extraList ) if extraList else None
    # we have all arguments so now we are going to create a list with as much dictionary as there is files
    if not ret :
      ret = S_OK()
    if actionArgs :
      for actionArgId  in range( len( actionArgs ) ) :
        actionArg = actionArgs[actionArgId]
        mergedDict = commonArgs.copy()
        mergedDict.update( actionArg )
        mergedDict['extra'] = '%s' % ( actionArg.get( 'extra', '' ) if actionArg.get( 'extra' ) else '' + \
                                 commonArgs.get( 'extra' ) if commonArgs.get( 'extra' ) else '' )
        actionArgs[actionArgId] = mergedDict
    else :
        actionArgs = [commonArgs]

    ret['Value'] = actionArgs
  return ret

def extractArgsSetReplicaProblematic( argsDecorator, *args, **kwargs ):
  """ this is the special function to extract args for the SetReplicaProblematic method from StorageElement
      the structure of args is { 'lfn':{'targetse' : 'PFN',....} , ...}
  """
  try :

    argsPosition = argsDecorator['argsPosition']
    commonArgs = dict.fromkeys( wantedArgs, None )
    extraList = []
    actionArgs = []
    if kwargs:
      for key in kwargs:
        extraList.append( "%s = %s" % ( key, kwargs[key] ) )
    # in args it should be only one argument, a dictionary
    for i in range( len( argsPosition ) ):
      if argsPosition[i] is 'files':
        for key, dictInfo in args[i].items():
          for key2, value in dictInfo.items():
            argDict = dict( commonArgs )
            argDict['file'] = key
            argDict['targetSE'] = key2
            argDictExtra = list( extraList )
            argDictExtra.append( 'PFN = %s' % value )
            argDict['extra'] = ','.join( argDictExtra )
            actionArgs.append( argDict )
  except Exception as e:
    gLogger.error( 'unexpected error in DLFucntions.extractArgsSetReplicaProblematic %s' % e )
    ret = S_ERROR( 'unexpected error in DLFucntions.extractArgsSetReplicaProblematic %s' % e )
    ret['Value'] = actionArgs
    return ret
  return S_OK( actionArgs )


def extractArgsTuple( argsDecorator, *args, **kwargs ):
  """this is the special function to extract arguments from a decorate function
    when the decorate function has tuple in arguments like 'registerFile' in the data manager
  """
  try :

    actionArgs = []
    commonArgs = dict.fromkeys( wantedArgs, None )

    argsPosition = argsDecorator['argsPosition']
    tupleArgsPosition = argsDecorator['tupleArgsPosition']
    extraList = []
    for i in range( len( argsPosition ) ):
      if i == len( args ):
        break
      a = argsPosition[i]
      if a in wantedArgs:
        if a is dl_files:
          commonArgs[dl_files] = checkArgumentFormat( args[i] )
        else :
          commonArgs[a] = args[i]
      else:
        if a is dl_tuple:
          tupleArgs = list()
          dictExtract = dict( argsDecorator )
          dictExtract['argsPosition'] = tupleArgsPosition
          if isinstance( args[i], list ):
            for t in args[i]:
              a = extractArgs( dictExtract, *t )['Value']
              tupleArgs.append( a[0] )
          elif isinstance( args[i], tuple ) :
            if isinstance( args[i][0], tuple ) :
              for t in args[i]:
                a = extractArgs( dictExtract, *t )['Value']
                tupleArgs.append( a[0] )
            else :
              a = extractArgs( dictExtract, *args[i] )['Value']
              tupleArgs.append( a[0] )
        elif a is not 'self':
          extraList.append( "%s = %s" % ( a, args[i] ) )

    for tupleArg in tupleArgs:
      actionArgs.append( mergeDictTuple( commonArgs, tupleArg, extraList ) )
  except Exception as e:
    gLogger.error( 'unexpected error in DLFucntions.getTupleArgs %s' % e )
    ret = S_ERROR( 'unexpected error in DLFucntions.getTupleArgs %s' % e )
    ret['Value'] = actionArgs
    return ret

  return S_OK( actionArgs )


def extractArgsExecuteFC( argsDecorator, *args, **kwargs ):
  """ this is the special function to extract arguments from a decorate function
      when the decorate function is 'execute' from the file catalog
      this is a special function because we need to get some information which
      are not passed in the decorate function like the function's name called
      to get the argument's position of the function that we really want to call
  """
  try :
    methodName = argsDecorator['methodName']
    if methodName in argsDecorator['methods_to_log']:

      info = argsDecorator['methods_to_log'][methodName]
      argsDecoratorWithInfo = argsDecorator.copy()
      argsDecoratorWithInfo.update( info )
      del argsDecoratorWithInfo[ 'methods_to_log' ]

      if info.get( 'specialFunction', '' ) == 'setReplicaProblematic' :
        args = extractArgsSetReplicaProblematic( argsDecoratorWithInfo , *args, **kwargs )['Value']
      else :
        argsDecoratorWithInfo = argsDecorator.copy()
        argsDecoratorWithInfo.update( info )
        del argsDecoratorWithInfo[ 'methods_to_log' ]
        args = extractArgs( argsDecoratorWithInfo , *args, **kwargs )['Value']
    else:
      raise NoLogException( 'Method %s  is not into the list of method to log' % methodName )
  except Exception as e:
    ret = S_ERROR( 'unexpected error in DLFucntions.getArgsExecuteFC %s' % e )
    ret['Value'] = args['Value']
    return ret

  return S_OK( args )

def extractArgsExecuteSE( argsDecorator, *args, **kwargs ):
  """ this is the special function to extract arguments from a decorate function
      when the decorate function is 'execute' from the Storage Element
      this is a special function because we need to get some information which
      are not passed in the decorate function like the function's name
  """
  actionArgs = []
  try :
    methodName = argsDecorator['methodName']
    if methodName in argsDecorator['methods_to_log']:
      info = argsDecorator['methods_to_log'][methodName]
      actionArgs = extractArgs( info , *args, **kwargs )['Value']
      for arg in actionArgs :
        arg['targetSE'] = argsDecorator['targetSE']
    else:
      raise NoLogException( 'Method %s is not into the list of method to log' % methodName )
  except NoLogException :
    raise
  except Exception as e:
    gLogger.error( 'unexpected error in DLFucntions.getArgsExecuteSE %s' % e )
    ret = S_ERROR( 'unexpected error in DLFucntions.getArgsExecuteSE %s' % e )
    ret['Value'] = actionArgs
    return ret

  return S_OK( actionArgs )

def mergeDictTuple( opArgs, tupleArgs, extraList ):
  """merge of two dict which contains arguments needed to create actions"""
  try :
    localExtraList = list( extraList )
    mergedDict = dict()

    for key in set( opArgs.keys() + tupleArgs.keys() ) :

      argList = list()

      if key in opArgs:
        if opArgs[key] :
          if isinstance( opArgs[key], list ):
            for val in opArgs[key]:
              argList.append( val )
          else :
            argList.append( opArgs[key] )

      if key in tupleArgs :
        if tupleArgs[key] :
          if isinstance( tupleArgs[key], list ):
            for val in tupleArgs[key]:
              argList.append( val )
          else :
            argList.append( tupleArgs[key] )

      if key is 'files' :
        mergedDict['file'] = tupleArgs['file']
      else :
        if len( argList ) == 0 :
          mergedDict[key] = None
        else :
          mergedDict[key] = ','.join( argList )

    localExtraList.append( tupleArgs['extra'] )
    if localExtraList:
      mergedDict['extra'] = ','.join( localExtraList )
    else:
      mergedDict['extra'] = None
  except Exception as e:
    gLogger.error( 'unexpected error in DLFucntions.mergeDictTuple %s' % e )
    raise

  return mergedDict
