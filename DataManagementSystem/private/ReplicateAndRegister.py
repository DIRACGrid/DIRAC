########################################################################
# $HeadURL $
# File: ReplicateAndRegister.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/13 18:49:12
########################################################################
""" :mod: ReplicateAndRegister 
    =======================
 
    .. module: ReplicateAndRegister
    :synopsis: ReplicateAndRegister operation handler 
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    ReplicateAndRegister operation handler 
"""
__RCSID__ = "$Id $"
##
# @file ReplicateAndRegister.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/13 18:49:28
# @brief Definition of ReplicateAndRegister class.

## imports 
from DIRAC import S_OK, S_ERROR
from DIRAC.RequestManagementSystem.private.OperationHandler import OperationHandler

########################################################################
class ReplicateAndRegister( OperationHandler ):
  """
  .. class:: ReplicateAndRegister

  ReplicateAndRegister operation handler
  """

  def __init__( self, operation ):
    """c'tor

    :param self: self reference
    """
    OperationHandler.__init__( self, operation )

  def __call__( self ):
    """ call me maybe """    
    ## list of targetSEs
    targetSEs = list( set( [ targetSE.strip() for targetSE in self.operation.TargetSE 
                             if targetSE.strip() ] ) )
    sourceSE = self.operation.SourceSE if self.operation.SourceSE else ""
    
    for targetSE in targetSEs:
      for opFile in self.operation:
        if opFile.Status != "Waiting":
          continue
      


