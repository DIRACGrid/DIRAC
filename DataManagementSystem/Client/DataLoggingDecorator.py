'''
Created on May 4, 2015

@author: Corentin Berger
'''

import functools, types, sys, time, random

from types import StringTypes
from threading import current_thread

from DIRAC import gLogger

from DIRAC.Core.Utilities                           import DEncode
from DIRAC.RequestManagementSystem.Client.Request   import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
# from dls
import DIRAC.DataManagementSystem.Client.DataLogging.DLUtilities as DLUtilities
from DIRAC.DataManagementSystem.Client.DataLogging.DLUtilities import getCallerName
from DIRAC.DataManagementSystem.Client.DataLogging.DLAction import DLAction
from DIRAC.DataManagementSystem.Client.DataLogging.DLThreadPool import DLThreadPool
from DIRAC.DataManagementSystem.Client.DataLogging.DLFile import DLFile
from DIRAC.DataManagementSystem.Client.DataLogging.DLStorageElement import DLStorageElement
from DIRAC.DataManagementSystem.Client.DataLogging.DLMethodName import DLMethodName
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
from DIRAC.DataManagementSystem.Client.DataLogging.DLException import DLException, NoLogException


# wrap _DLDecorator to allow passing some arguments to the decorator
def DataLoggingDecorator( function = None, **kwargs ):
  if function:
    # with no argument for the decorator the call is like decorator(func)
      return _DataLoggingDecorator( function )
  else:
    # if the decorator has some arguments, the call is like that decorator(args)(func)
    # so function will be none, we can get it with a wrapper
    def wrapper( function ):
      return _DataLoggingDecorator( function, **kwargs )
    return wrapper


class _DataLoggingDecorator( object ):
  """ decorator for data logging in DIRAC
      the aim of this decorator is to know all operations done about a Dirac LFN
      for this, the decorator get arguments from the called of the decorated method
      create a DLMethodCall which is an operation on a single lfn or multiple lfn
      then create as much DLAction as lfn
      then call the decorated method and get the result to update the status of each action
      if an exception is raised by the decorated function, the exception is raised by the decorator
      if an exception is raised due to the decorator, it's like nothing happened for the decorated method
      only works with method


      for this to work, you have to pass some arguments to the decorator
      the first arguments to pass is a list with the arguments positions in the decorated method
      for example for the putAndRegister method you have to pass argsPosition = ['self', 'files', 'localPath', 'targetSE' ]
      in the decorator
      some keywords are very important like files, targetSE and srcSE, keywords are in DLUtilities file
      so if the parameter of the decorated Function is 'sourceSE' you have to write 'srcSE' in the argsPosition's list
      if the parameter of the decorated Function is 'lfns' you have to write 'files' in the argsPosition's list

      next you have to tell to the decorator which function you want to called to extract arguments
      for example getActionArgsFunction = 'tuple', there is a dictionary to map keywords with functions to extract arguments

      you can pass as much arguments as you want to the decorator

      Common arguments are :
        argsPosition : a list of arguments names, to know which argument it is on each position, if the argument can be passed in kwargs
                        and its key is not the same name we want to save, you need to pass a tuple ( 'specialKeyWord', 'keyInKwargs' )
        getActionArgsFunction : a string to know which function will be used to extract args, possibilities are in funcDict
        tupelPostion : a list of arguments names, to describe tuple
  """

  def __init__( self, func , **kwargs ):
    """
      func is the decorated function
      ** kwargs nominated arguments for the decorator

      *args is always empty, do not use it
    """
    # we set the function and it name
    self.func = func
    self.name = func.__name__

    # we create a dictionary to save the kwargs arguments passed to the decorator itself, because after we need this arguments
    self.argsDecorator = {}

    # by default the insertion is not direct, we insert compressed sequence and a periodic task insert it in database
    # if we want a direct insertion, without the periodic task, we have to pass directInsert = True into the arguments of the decorator
    self.argsDecorator['directInsert'] = False

    # set the different attribute from kwargs
    for key, value in kwargs.items():
      if type( value ) in StringTypes:
        value = value.encode()
      if value is not None:
        self.argsDecorator[key] = value

    # here we get the function to parse arguments to create action
    try :
      self.getActionArgsFunction = getattr( DLUtilities, 'extractArgs' + self.argsDecorator.get( 'getActionArgsFunction', '' ) )
    except :
      self.getActionArgsFunction = getattr( DLUtilities, 'extractArgs' )
    # this permits to replace all special info like docstring of func in place of self, included the name
    functools.wraps( func )( self )

  def __get__( self, inst, owner = None ):
    """
      inst is the instance of the object who called the decorated function
    """
    self.inst = inst
    # bind the new function ( the decorated function) to the instance
    ret = types.MethodType( self, inst )
    return ret

  def __call__( self, *args, **kwargs ):
    """ method called each time when a decorated function is called
        get information about the function and create a sequence of method calls
    """
    result = None
    exception = None
    isCalled = False
    isMethodCallCreated = False

    try:
      # we set the caller
      self.setCaller()
      # sometimes we need an attribute into the object who called the decorated method
      # we will get it here and add it in the local argsDecorator dictionary
      # we need a local dictionary because of the different calls from different threads
      # for example when the decorated method is _execute , the real method called is contained into the object
      # this will not work with a function because the first arguments in args should be the self reference of the object
      localArgsDecorator = self.getAttributeFromObject( args[0] )

      # we get the arguments from the call of the decorated method to create the DLMethodCall object
      methodCallArgsDict = self.getMethodCallArgs( localArgsDecorator )

      # get args for the DLAction objects
      actionArgs = self.getActionArgs( localArgsDecorator, *args, **kwargs )

      # create and append methodCall into the sequence of the thread
      methodCall = self.createMethodCall( methodCallArgsDict )
      isMethodCallCreated = True

      # initialization of the DLActions with the different arguments, set theirs status to 'unknown'
      self.initializeActions( methodCall, actionArgs )

      try :
        # isCalled is False until the decorated method is called
        # like that if there is an exception, we know if we have to call it or no
        isCalled = True
        # call of the func, result is the return of the decorated function
        result = self.func( *args, **kwargs )
      except Exception as e:
        exception = e
        raise
    except NoLogException :
      if not isCalled :
        result = self.func( *args, **kwargs )
    except DLException as e:
      if not isCalled :
        result = self.func( *args, **kwargs )
      gLogger.error( 'unexpected Exception in DLDecorator.call %s' % e )
    finally:
      try :
        if isMethodCallCreated :
          # now we set the status ( failed or successful) of methodCall's actions
          self.setActionStatus( result, methodCall, exception )
          # pop of the methodCall corresponding to the decorated method
          self.popMethodCall()
        # if the sequence is complete we insert it into DB
        if self.isSequenceComplete() :
          self.insertSequence()
      except Exception as e :
        gLogger.error( 'unexpected Exception in DLDecorator.call %s' % e )
    return result

  def setActionStatus( self, foncResult, methodCall, exception ):
    """ set the status of each action of a method call
      :param foncResult: result of a decorated function
      :param methodCall: methodCall in which we have to update the status of its actions


      foncResult can be :
       {'OK': True, 'Value': {'Successful': {'/data/file3': {}, '/data/file1': {}}, 'Failed': {'/data/file2': {}, '/data/file4': {}}}}
       {'OK': True, 'Value':''}
       {'OK': True, 'Value':{}}
       {'OK': False, 'Message':'a message'}
    """
    # by default all status are Unknown
    try :
      if not exception  :
        if isinstance( foncResult, dict ):
          if foncResult['OK']:
            if isinstance( foncResult['Value'], dict ) and \
               ( 'Successful' in foncResult['Value'] ) and \
               ( 'Failed' in foncResult['Value'] ) :
            # get the success and the fail
              successful = foncResult['Value']['Successful']
              failed = foncResult['Value']['Failed']
              for action in methodCall.actions :
                if action.file.name in successful :
                  action.status = 'Successful'
                elif action.file.name in failed :
                  action.status = 'Failed'
                  action.errorMessage = str( foncResult['Value']['Failed'][action.file.name] )
            else:
              for action in methodCall.actions :
                action.status = 'Successful'

          else :  # if  not ok
            for action in methodCall.actions :
              action.status = 'Failed'
              action.errorMessage = foncResult['Message']

        else :  # if not a dict
          gLogger.error( 'the result of a function is not a dict, you have to use S_OK and S_ERROR' )
      else :
        # exception not None
        for action in methodCall.actions :
          action.status = 'Failed'
          action.errorMessage = str( exception )
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.getActionStatus %s' % e )
      raise DLException( e )

  def initializeActions( self, methodCall, actionsArgs ):
    """ create all action for a method call and initialize their status to value 'Unknown'

        :param methodCall : methodCall in which we have to initialize action
        :param actionArgs : arguments to create the action, it's a list of dictionary
    """
    try :
      for actionArgs in actionsArgs :
        methodCall.addAction( DLAction( DLFile( actionArgs['file'] ), 'Unknown',
              DLStorageElement( actionArgs['srcSE'] ), DLStorageElement( actionArgs['targetSE'] ),
              actionArgs['extra'], None, None ) )
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.initializeActions %s' % e )
      raise DLException( e )

  def createMethodCall( self, args ):
    """ create a method call and add it into the sequence corresponding to its thread
    :param args : a dict with the arguments needed to create a methodcall
    """
    try :
      methodCall = DLThreadPool.getDataLoggingSequence( current_thread().ident ).appendMethodCall( args )
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.createMethodCall %s' % e )
      raise DLException( e )
    return methodCall

  def popMethodCall( self ):
    """ pop a methodCall from the sequence corresponding to its thread """
    try :
      _methodCall = DLThreadPool.getDataLoggingSequence( current_thread().ident ).popMethodCall()
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.popMethodCall %s' % e )
      raise DLException( e )

  def isSequenceComplete( self ):
    return DLThreadPool.getDataLoggingSequence( current_thread().ident ).isComplete()

  def setCaller( self ):
    """ set the caller of the sequence corresponding to its thread
        first we tried to get the caller
        next if the caller is not set, we set it
    """
    try :
      if not DLThreadPool.getDataLoggingSequence( current_thread().ident ).isCallerSet():
        DLThreadPool.getDataLoggingSequence( current_thread().ident ).setCaller( getCallerName() )
    except Exception as e:
      gLogger.error( 'unexpected Exception in DataLoggingDecorator.setCaller %s' % e )
      raise DLException( e )

  def getMethodCallArgs( self, localArgsDecorator ):
    """ get arguments to create a method call
        :return methodCallDict : contains all the arguments to create a method call
    """
    try :
      methodCallDict = {}
      if hasattr( self, 'inst' ) :
        # the decorated function is a method
        methodCallDict['name'] = DLMethodName( localArgsDecorator.get( 'className', self.inst.__class__ .__name__ )\
                                    + '.' + localArgsDecorator.get( 'methodName', self.name ) )
      else :
        # the decorated function is a function
        module = sys.modules[self.func.__module__]
        methodCallDict['name'] = DLMethodName( module.__name__ + '.' + self.func.__name__ )
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.getMethodCallArgs %s' % e )
      raise DLException( e )
    return methodCallDict


  def getAttributeFromObject( self, obj ) :
    """ get attributes from an object
        add this attributes to the dict which contains all arguments of the decorator
    """
    localDict = dict( self.argsDecorator )
    try :
      for keyword, attrName in self.argsDecorator.get( 'attributesToGet', {} ).items():
        localDict[keyword] = getattr( obj, attrName, None )
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.getAttribute %s' % e )
      raise DLException( e )
    return localDict

  def getActionArgs( self, argsDecorator, *args, **kwargs ):
    """ this method is here to call the function to get arguments of the decorated function
        we don't call directly this function because if an exception is raised we need to raise a specific exception
    """
    try :
      ret = self.getActionArgsFunction( argsDecorator, *args, **kwargs )
    except NoLogException :
      raise
    if not ret['OK']:
      gLogger.error( 'unexpected error in DLDecorator.getActionArgs %s' % ret['Message'] )
    ret = ret['Value']

    return ret

  def insertSequence( self ):
    """ this method call method named insertSequence from DLClient
        to insert a sequence into database
    """
    seq = DLThreadPool.popDataLoggingSequence( current_thread().ident )
    try :
      client = DataLoggingClient()
      res = client.insertSequence( seq, self.argsDecorator['directInsert'] )
      if not res['OK'] :
        rpcstub = list( res[ 'rpcStub' ] )
        arguments = list( rpcstub[2] )
        arguments[0] = seq.toJSON()['Value']
        arguments = tuple( arguments )
        rpcstub[2] = arguments
        rpcstub = tuple( rpcstub )
        request = Request()
        request.RequestName = "DataManagement.DataLogging.%s.%s" % ( time.time(), random.random() )
        forwardDISETOp = Operation()
        forwardDISETOp.Type = "ForwardDISET"
        forwardDISETOp.Arguments = DEncode.encode( rpcstub )
        request.addOperation( forwardDISETOp )
        res = ReqClient().putRequest( request )
    except Exception as e:
      gLogger.error( 'unexpected Exception in DLDecorator.insertSequence %s' % e )
      raise


