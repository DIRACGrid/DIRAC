## $HeadURL $
#''' Decorators
#
#  Syntactic sugar used in the RSS, only in the DB. Eventually will dissapear.
#
#'''
#
#import types
#
#from DIRAC import S_ERROR
#
#__RCSID__  = '$Id: $'
#
#class BaseDec( object ):
#
#  def __init__( self, f ):
#    self.f = f
#    
#  def __get__( self, obj, objtype = None ):
#    return types.MethodType( self, obj, objtype ) 
#
#  def __call__( self, *args, **kwargs ):
#    # Do it yourself !
#    pass
#
#################################################################################
#
#class CheckDBExecution( BaseDec ):
#  
#  def __call__( self, *args, **kwargs ):
#    
#    try:
#      return self.f( *args, **kwargs )
#    except Exception, x:
#      return S_ERROR( x )
#
#################################################################################
#
#class ValidateDBTypes( BaseDec ):
#  
#  def __call__( self, *args, **kwargs ):  
#  
#    if not isinstance( args[1], dict ):
#      return S_ERROR( 'args MUST be a dict, not %s' % type( args[1] ))
#    if not isinstance( args[2], dict ):
#      return S_ERROR( 'kwargs MUST be a dict, not %s' % type( args[2] ))
#    return self.f( *args, **kwargs )
#           
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    