from DIRAC import S_OK
class AbsState( object ):

  stateName = None
  
  def __init__( self, stateManager, ownerObject ):
    self.ownerObject = ownerObject
    self.stateManager = stateManager

      
class AbsStateManager( object ):
  
  # Map the name of the state to the one of the class
  _stateNameToClass = {}
  _allowedTransitions = {}
  _initialState = None
  _currentState = None

  def __init__( self, ownerObject, initialState = None ):
    self.ownerObject = ownerObject
    initialState = initialState if initialState else self._initialState
    self._currentState = self._getStateClass( initialState )( self, ownerObject )
  
  def _getStateClass( self, stateName ):
    return self._stateNameToClass[stateName]

  def goToState( self, nextState, *args, **kwargs ):
    if nextState not in self._allowedTransitions[self._currentState.stateName]:
      raise Exception( 'Transition not allowed' )

    res = S_OK()
    if hasattr( self._currentState, '_transition%s' % nextState ):
      met = getattr( self._currentState, '_transition%s' % nextState )
      res = met( *args, **kwargs )

    if res['OK']:
      newStateClass = self._getStateClass( nextState )
      newStateObj = newStateClass( self, self.ownerObject )
      self._currentState = newStateObj
      
    return res

  def getStateName( self ):
    return self._currentState.stateName
    

class FTS3JobState_New(AbsState):
  stateName = 'NEW'

  def _transitionSUBMITTED(self):
    """ Transition to the SUBMITTED state.
        * prepare the submission
        * perform the submission
    """

    res = self.ownerObject._prepareSubmission()

    if not res['OK']:
      return res

    res = self.ownerObject._submit()

    return res
      

class FTS3JobState_Submitted( AbsState ):
  stateName = 'SUBMITTED'
  pass

class FTS3JobState_Ready( AbsState ):
  pass

class FTS3JobState_Active( AbsState ):
  pass

class FTS3JobState_Canceled( AbsState ):
  pass

class FTS3JobState_Failed( AbsState ):
  pass

class FTS3JobState_Finished( AbsState ):
  pass

class FTS3JobState_FinishedDirty( AbsState ):
  pass

class FTS3JobState_Done( AbsState ):
  pass

class FTS3JobStateManager(AbsStateManager):
  
  _stateNameToClass = {'NEW' : FTS3JobState_New,
                        'SUBMITTED' : FTS3JobState_Submitted,
                        'READY' : FTS3JobState_Ready,
                        'ACTIVE' : FTS3JobState_Active,
                        'CANCELED' : FTS3JobState_Canceled,
                        'FAILED' : FTS3JobState_Failed,
                        'FINISHED' : FTS3JobState_Finished,
                        'FINISHEDDIRTY' : FTS3JobState_FinishedDirty,
                        'DONE' : FTS3JobState_Done,
                        }

  _initialState = 'NEW'

  _allowedTransitions = {'NEW' : ['SUBMITTED', 'CANCELED'],
                        'SUBMITTED' : ['READY', 'CANCELED'],
                        'READY' : ['ACTIVE', 'CANCELED'],
                        'ACTIVE' : ['CANCELED', 'FINISHED', 'FAILED', 'FINISHEDDIRTY'],
                        'CANCELED' : [],
                        'FAILED' : ['DONE', 'NEW'],
                        'FINISHED' : ['DONE'],
                        'FINISHEDDIRTY' : ['DONE', 'NEW'],
                        'DONE' : [],
                       }


class FTS3FileState_New( AbsState ):
  pass

class FTS3FileState_Submitted( AbsState ):
  pass

class FTS3FileState_Ready( AbsState ):
  pass

class FTS3FileState_Active( AbsState ):
  pass

class FTS3FileState_Canceled( AbsState ):
  pass

class FTS3FileState_Staging( AbsState ):
  pass

class FTS3FileState_Failed( AbsState ):
  pass

class FTS3FileState_Finished( AbsState ):
  pass

class FTS3FileState_Done( AbsState ):
  pass

class FTS3FileStateManager( AbsStateManager ):

  _stateNameToClass = {'NEW' : FTS3FileState_New,
                        'SUBMITTED' : FTS3FileState_Submitted,
                        'READY' : FTS3FileState_Ready,
                        'STAGING' : FTS3FileState_Staging,
                        'ACTIVE' : FTS3FileState_Active,
                        'CANCELED' : FTS3FileState_Canceled,
                        'FAILED' : FTS3FileState_Failed,
                        'FINISHED' : FTS3FileState_Finished,
                        'DONE' : FTS3FileState_Done,
                       }

  _initialState = 'NEW'

  _allowedTransitions = {'NEW' : ['SUBMITTED', 'CANCELED'],
                        'SUBMITTED' : ['READY', 'CANCELED'],
                        'READY' : ['ACTIVE', 'CANCELED', 'STAGING'],
                        'STAGING' : ['ACTIVE', 'CANCELED', 'FAILED'],
                        'ACTIVE' : ['CANCELED', 'FINISHED', 'FAILED'],
                        'CANCELED' : [],
                        'FAILED' : [''],
                        'FINISHED' : ['DONE'],
                        'DONE' : [],
                       }


class FTS3OperationState_New( AbsState ):
  pass


class FTS3OperationState_Active( AbsState ):
  pass

class FTS3OperationState_Finished( AbsState ):
  pass

class FTS3OperationState_Done( AbsState ):
  pass

class FTS3OperationState_Failed( AbsState ):
  pass

class FTS3OperationState_Canceled( AbsState ):
  pass

class FTS3OperationStateManager( AbsStateManager ):

  _stateNameToClass = {'NEW' : FTS3OperationState_New,
                        'ACTIVE' : FTS3OperationState_Active,
                        'CANCELED' : FTS3OperationState_Canceled,
                        'FAILED' : FTS3OperationState_Failed,
                        'FINISHED' : FTS3OperationState_Finished,
                        'DONE' : FTS3OperationState_Done,
                       }

  _initialState = 'NEW'

  _allowedTransitions = {'NEW' : ['ACTIVE', 'CANCELED'],
                        'ACTIVE' : ['CANCELED', 'FINISHED', 'FAILED'],
                        'CANCELED' : [],
                        'FAILED' : [''],
                        'FINISHED' : ['DONE'],
                        'DONE' : [],
                       }

