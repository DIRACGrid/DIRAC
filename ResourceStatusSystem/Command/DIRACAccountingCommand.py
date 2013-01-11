# $HeadURL:  $
''' DIRACAccountingCommand
 
  The DIRACAccountingCommand class is a command class to 
  interrogate the DIRAC Accounting.
  
'''

from datetime import datetime, timedelta

from DIRAC                                                      import S_OK, S_ERROR
from DIRAC.AccountingSystem.Client.ReportsClient                import ReportsClient
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

__RCSID__ = '$Id:  $'

################################################################################
################################################################################

class DIRACAccountingCommand( Command ):
  
  def __init__( self, args = None, clients = None ):
    
    super( DIRACAccountingCommand, self ).__init__( args, clients )
    
    if 'ReportGenerator' in self.apis:
      self.rgClient = self.apis[ 'ReportGenerator' ]
    else:
      self.rgClient = RPCClient( 'Accounting/ReportGenerator' ) 

    if 'ReportsClient' in self.apis:
      self.rClient = self.apis[ 'ReportsClient' ]
    else:
      self.rClient = ReportsClient() 

    self.rClient.rpcClient = self.rgClient
  
  def doCommand( self ):
    """ 
    Returns jobs accounting info for sites in the last 24h
    `args`: 
       - args[0]: string - should be a ValidElement
       
       - args[1]: string - should be the name of the ValidElement
       
       - args[2]: string - should be 'Job' or 'Pilot' or 'DataOperation'
         or 'WMSHistory' (??) or 'SRM' (??)
       
       - args[3]: string - should be the plot to generate (e.g. CPUEfficiency) 
       
       - args[4]: dictionary - e.g. {'Format': 'LastHours', 'hours': 24}
       
       - args[5]: string - should be the grouping
       
       - args[6]: dictionary - optional conditions
    """

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
          return S_ERROR( '%s is not a valid granularity' % granularity )
      elif accounting == 'DataOperation':
        conditions[ 'Destination' ] = [ name ]

    return self.rClient.getReport( accounting, plot, fromT, toT, conditions, grouping )
        
################################################################################
################################################################################

class TransferQualityCommand( Command ):

  def __init__( self, args = None, clients = None ):
    
    super( TransferQualityCommand, self ).__init__( args, clients )
    
    if 'ReportGenerator' in self.apis:
      self.rgClient = self.apis[ 'ReportGenerator' ]
    else:
      self.rgClient = RPCClient( 'Accounting/ReportGenerator' ) 

    if 'ReportsClient' in self.apis:
      self.rClient = self.apis[ 'ReportsClient' ]
    else:
      self.rClient = ReportsClient() 

    self.rClient.rpcClient = self.rgClient

  def doCommand( self ):
    """ 
    Return getQuality from DIRAC's accounting ReportsClient
    
    `args`: a tuple
      - args[0]: string: should be a ValidElement

      - args[1]: string should be the name of the ValidElement

      - args[2]: optional dateTime object: a "from" date
    
      - args[3]: optional dateTime object: a "to" date
      
    :returns:
      {'Result': None | a float between 0.0 and 100.0}
    """

    if not 'fromDate' in self.args:
      fromDate = datetime.utcnow() - timedelta( hours = 2 )
    else:  
      fromDate = self.args[ 'fromDate' ]

    if not 'toDate' in self.args:
      toDate = datetime.utcnow()
    else:
      toDate = self.args[ 'toDate' ]

    if not 'name' in self.args:
      return S_ERROR( 'name not specified' )
    name = self.args[ 'name' ]

    results = self.rClient.getReport( 'DataOperation', 'Quality', fromDate, toDate, 
                                      { 'OperationType' : 'putAndRegister', 
                                        'Destination'   : [ name ] 
                                      }, 'Channel' )
      
    if not results[ 'OK' ]:
      return results
    
    pr_q_d = results[ 'Value' ][ 'data' ]
    
    #FIXME: WHAT the hell is this doing ?
    values = []
    if len( pr_q_d ) == 1:
      
      for k in pr_q_d.keys():
        for n in pr_q_d[ k ].values():
          values.append( n )
      res = sum( values ) / len( values )    

    else:
      for n in pr_q_d[ 'Total' ].values():
        values.append(n)
      res = sum( values ) / len( values )

    return S_OK( res )      
    
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
#       - args[0]: string: should be a ValidElement
#  
#       - args[1]: string should be the name of the ValidElement
#
#    :returns:
#      {'Result': None | a float between 0.0 and 100.0}
#    """
#    
#    super(TransferQualityCached_Command, self).doCommand()
#    self.apis = initAPIs( self.__APIs__, self.apis )  
#      
#    name = self.args[1]
#    
#    try:
#      res = self.apis[ 'ResourceManagementClient' ].getCachedResult(name, 'TransferQualityEverySEs', 'TQ', 'NULL')
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

class CachedPlotCommand( Command ):

  def __init__( self, args = None, clients = None ):
    
    super( CachedPlotCommand, self ).__init__( args, clients )
    
    if 'ResourceManagementClient' in self.apis:
      self.rmClient = self.apis[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient() 
  
  def doCommand( self ):
    """ 
    Returns transfer quality plot as it is cached in the accounting cache.

    :attr:`args`: 
       - args[0]: string - should be a ValidElement
  
       - args[1]: string - should be the name of the ValidElement

       - args[2]: string - should be the plot type

       - args[3]: string - should be the plot name

    :returns:
      a plot
    """

    if not 'element' in self.args:
      return S_ERROR( 'element no specified' )
    element = self.args[ 'element' ]

    if not 'name' in self.args:
      return S_ERROR( 'Name no specified' )
    name = self.args[ 'name' ]
    
    if not 'plotType' in self.args:
      return S_ERROR( 'plotType no specified' )
    plotType = self.args[ 'plotType' ]
    
    if not 'plotName' in self.args:
      return S_ERROR( 'plotName no specified' )
    plotName = self.args[ 'plotName' ]
   
    #FIXME: we have no any longer Service granularity !      
    if element == 'Service':
      name = name.split('@')[1]
    
    meta = { 'columns' : 'Result' }  
      
    results = self.rmClient.selectAccountingCache( name = name, plotType = plotType,
                                                   plotName = plotName, meta = meta )
      
    if not results[ 'OK' ]:
      return results      
    results = results[ 'Value' ]
      
    if results == []:
      results = { 'data' : {}, 'granularity' : 900 }
    else:
      #FIXME: WTH is an eval doing here !!!!
      results = eval( results[0] )

    return results
    
################################################################################
################################################################################

class TransferQualityFromCachedPlotCommand( Command ):

  def __init__( self, args = None, clients = None ):
    
    super( TransferQualityFromCachedPlotCommand, self ).__init__( args, clients )
    
    if 'ResourceManagementClient' in self.apis:
      self.rmClient = self.apis[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient() 
  
  def doCommand( self ):
    """ 
    Returns transfer quality from the plot cached in the accounting cache.

    :attr:`args`: 
       - args[0]: string: should be a ValidElement
  
       - args[1]: string should be the name of the ValidElement

    :returns:
      {'Result': None | a float between 0.0 and 100.0}
    """
    
    if not 'name' in self.args:
      return S_ERROR( 'Name no specified' )
    name = self.args[ 'name' ]
    
    if not 'plotType' in self.args:
      return S_ERROR( 'plotType no specified' )
    plotType = self.args[ 'plotType' ]
    
    if not 'plotName' in self.args:
      return S_ERROR( 'plotName no specified' )
    plotName = self.args[ 'plotName' ]
             
    meta = { 'columns' : 'Result' } 
      
    results = self.rmClient.selectAccountingCache( name = name, plotType = plotType,
                                                   plotName = plotName, meta = meta )
    
    if not results[ 'OK' ]:
      return results
    results = results[ 'Value' ]

    if results == []:
      results = None
    else: 
      #FIXME: remove the eval from here !!
      results = eval( results[ 0 ][ 0 ] )
      
      num, den = 0, 0
      
      se = results[ 'data' ].keys()[ 0 ]
      
      num = num + len( results[ 'data' ][ se ] )
      den = den + sum( results[ 'data' ][ se ].values() )
      meanQuality = den / num
          
      results = meanQuality

    return S_OK( results )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF