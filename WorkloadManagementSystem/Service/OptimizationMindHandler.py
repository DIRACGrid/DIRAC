
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.ExecutorMindHandler import ExecutorMindHandler

class OptimizationMindHandler( ExecutorMindHandler ):

  @classmethod
  def initialize( self, serviceInfoDict ):
    #Load tasks!!
    return S_OK()

  def exec_dispatch( self, taskId, taskObj ):
    print "WE GOT HERE!!"

  def exec_serializeTask( self, taskId, taskObj ):
    raise Exception( "No exec_serializeTask defined!!" )

  def exec_deserializeTask( self, taskStub ):
    raise Exception( "No exec_deserializeTask defined!!" )

  def exec_taskError( self, taskId, errorMsg ):
    raise Exception( "No exec_taskError defined!!" )
