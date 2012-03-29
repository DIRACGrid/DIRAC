# $HeadURL $
''' DIRACAccounting_Command
 
  The DIRACAccounting_Command class is a command class to 
  interrogate the DIRAC Accounting.
  
'''

from datetime                                        import datetime, timedelta

from DIRAC                                           import gLogger, S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Command.Command      import *
from DIRAC.ResourceStatusSystem.Command.knownAPIs    import initAPIs
from DIRAC.ResourceStatusSystem.Utilities.Utils      import where

__RCSID__ = '$Id: $'

################################################################################
################################################################################

class DIRACAccounting_Command( Command ):
  
  __APIs__ = [ 'ReportGenerator', 'ReportsClient' ]
  
  def doCommand( self ):
    """ 
    Returns jobs accounting info for sites in the last 24h
    `args`: 
       - args[0]: string - should be a ValidRes
       
       - args[1]: string - should be the name of the ValidRes
       
       - args[2]: string - should be 'Job' or 'Pilot' or 'DataOperation'
         or 'WMSHistory' (??) or 'SRM' (??)
       
       - args[3]: string - should be the plot to generate (e.g. CPUEfficiency) 
       
       - args[4]: dictionary - e.g. {'Format': 'LastHours', 'hours': 24}
       
       - args[5]: string - should be the grouping
       
       - args[6]: dictionary - optional conditions
    """
    
    super( DIRACAccounting_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )
    self.APIs[ 'ReportsClient' ].rpcClient = self.APIs[ 'ReportGenerator' ]

    try:

      granularity = self.args[0]
      name        = self.args[1]
      accounting  = self.args[2]
      plot        = self.args[3]
      period      = self.args[4]
      grouping    = self.args[5]
    
      if period[ 'Format' ] == 'LastHours':
        fromT = datetime.utcnow() - timedelta( hours = period[ 'hours' ] )
        toT   = datetime.utcnow()
      elif period[ 'Format' ] == 'Periods':
        #TODO
        pass
        
      if self.args[6] is not None:
        conditions = self.args[6]
      else:
        conditions = {}
        if accounting == 'Job' or accounting == 'Pilot':
          if granularity == 'Resource':
            conditions[ 'GridCE' ] = [ name ]
          elif granularity == 'Service':
            conditions[ 'Site' ] = [ name.split('@').pop() ]
          elif granularity == 'Site':
            conditions[ 'Site' ] = [ name ]
          else:
            return { 'Result' : S_ERROR( '%s is not a valid granularity' % granularity ) }
        elif accounting == 'DataOperation':
          conditions[ 'Destination' ] = [ name ]

      res = self.APIs[ 'ReportsClient' ].getReport( accounting, plot, fromT, toT, conditions, grouping )
    
    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : res }      

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
################################################################################

class TransferQuality_Command( Command ):

  __APIs__ = [ 'ReportGenerator', 'ReportsClient' ]

  def doCommand( self ):
    """ 
    Return getQuality from DIRAC's accounting ReportsClient
    
    `args`: a tuple
      - args[0]: string: should be a ValidRes

      - args[1]: string should be the name of the ValidRes

      - args[2]: optional dateTime object: a "from" date
    
      - args[3]: optional dateTime object: a "to" date
      
    :returns:
      {'Result': None | a float between 0.0 and 100.0}
    """
    super( TransferQuality_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )    
    self.APIs[ 'ReportsClient' ].rpcClient = self.APIs[ 'ReportGenerator' ]

    try:

      if self.args[2] is None:
        fromD = datetime.utcnow()-timedelta(hours = 2)
      else:
        fromD = self.args[2]

      if self.args[3] is None:
        toD = datetime.utcnow()
      else:
        toD = self.args[3]

      res = self.APIs[ 'ReportsClient' ].getReport( 'DataOperation', 'Quality', fromD, toD, 
                                          { 'OperationType': 'putAndRegister', 
                                            'Destination'  : [ self.args[1] ] }, 
                                          'Channel' )
      
      if res['OK']:
    
        pr_q_d = res[ 'Value' ][ 'data' ]
    
        values = []
        if len( pr_q_d ) == 1:
          for k in pr_q_d.keys():
            for n in pr_q_d[ k ].values():
              values.append( n )
          res = S_OK( sum( values ) / len( values ) )    

        else:
          for n in pr_q_d['Total'].values():
            values.append(n)
          res = S_OK( sum( values ) / len( values ) )

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : res }      
  
  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
################################################################################
#
#class TransferQualityCached_Command(Command):
#  
#  __APIs__ = [ 'ResourceManagementClient' ]
#  
#  def doCommand(self):
#    """ 
#    Returns transfer quality as it is cached
#
#    :attr:`args`: 
#       - args[0]: string: should be a ValidRes
#  
#       - args[1]: string should be the name of the ValidRes
#
#    :returns:
#      {'Result': None | a float between 0.0 and 100.0}
#    """
#    
#    super(TransferQualityCached_Command, self).doCommand()
#    self.APIs = initAPIs( self.__APIs__, self.APIs )  
#      
#    name = self.args[1]
#    
#    try:
#      res = self.APIs[ 'ResourceManagementClient' ].getCachedResult(name, 'TransferQualityEverySEs', 'TQ', 'NULL')
#      if res == []:
#        return {'Result':None}
#    except:
#      gLogger.exception("Exception when calling ResourceManagementClient for %s" %(name))
#      return {'Result':'Unknown'}
#    
#    return {'Result':float(res[0])}
#
#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
#    
################################################################################
################################################################################

class CachedPlot_Command( Command ):

  __APIs__ = [ 'ResourceManagementClient' ]
  
  def doCommand( self ):
    """ 
    Returns transfer quality plot as it is cached in the accounting cache.

    :attr:`args`: 
       - args[0]: string - should be a ValidRes
  
       - args[1]: string - should be the name of the ValidRes

       - args[2]: string - should be the plot type

       - args[3]: string - should be the plot name

    :returns:
      a plot
    """

    super( CachedPlot_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs ) 
      
    try:  
      
      granularity = self.args[0]
      name        = self.args[1]
      plotType    = self.args[2]
      plotName    = self.args[3]
    
      if granularity == 'Service':
        name = name.split('@')[1]
    
      accountingDict = { 
                        'name'     : name,
                        'plotType' : plotType,
                        'plotName' : plotName
                   }
      kwargs     = { 'meta' : { 'columns'     : 'Result' } }
      accountingDict.update( kwargs )  
      
      res = self.APIs[ 'ResourceManagementClient' ].getAccountingCache( **accountingDict )
      
      if res[ 'OK' ]:      
        res = res[ 'Value' ]
      
        if res == []:
          res = S_OK( { 'data' : {}, 'granularity' : 900 } )
        else:
          res = S_OK( eval( res[0] ) )

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : res }

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
################################################################################

class TransferQualityFromCachedPlot_Command(Command):
  
  __APIs__ = [ 'ResourceManagementClient' ]
  
  def doCommand(self):
    """ 
    Returns transfer quality from the plot cached in the accounting cache.

    :attr:`args`: 
       - args[0]: string: should be a ValidRes
  
       - args[1]: string should be the name of the ValidRes

    :returns:
      {'Result': None | a float between 0.0 and 100.0}
    """
    
    super(TransferQualityFromCachedPlot_Command, self).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )     

    try:

      name        = self.args[1]
      plotType    = self.args[2]
      plotName    = self.args[3]
             
      accountingDict = { 
                        'name'     : name,
                        'plotType' : plotType,
                        'plotName' : plotName
                   }
      kwargs     = { 'meta' : { 'columns' : 'Result' } }
      accountingDict.update( kwargs )  
      
      res = self.APIs[ 'ResourceManagementClient' ].getAccountingCache( **accountingDict )
    
      if res['OK']:
        res = res[ 'Value']

        if res == []:
          res = S_OK( None )
        else: 
          res = eval(res[0][0])
      
          s,n = 0,0
          SE = res[ 'data' ].keys()[ 0 ]
      
          n = n + len(res['data'][SE])
          s = s + sum(res['data'][SE].values())
          meanQuality = s/n
          
          res = S_OK( meanQuality )

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : res }

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF