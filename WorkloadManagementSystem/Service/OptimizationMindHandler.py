
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Base.ExecutorMindHandler import ExecutorMindHandler

class OptimizationMindHandler( ExecutorMindHandler ):

  @classmethod
  def initialize( self, serviceInfoDict ):
    self.executeTask( 1, { 'path' : [], 'data' : "" } )
    return S_OK()

  @classmethod
  def exec_dispatch( self, taskId, taskObj ):
    print "WE GOT HERE!!"
    next = 'WorkloadManagement/WhateverAgent'
    if next in taskObj[ 'path' ]:
      return S_OK()
    return S_OK( next )

  @classmethod
  def exec_serializeTask( cls, taskObj ):
    return S_OK( DEncode.encode( taskObj ) )

  def exec_deserializeTask( self, taskStub ):
    result = DEncode.decode( taskStub )
    if result[1] != len( taskStub ):
      return S_ERROR( "Could not deserialize all stub!" )
    return S_OK( result[0] )

  def exec_taskError( self, taskId, errorMsg ):
    raise Exception( "No exec_taskError defined!!" )
