################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

""" 
  The GOCDBStatus_Command class is a command class to know about 
  present downtimes
"""

import urllib2
from datetime import datetime

from DIRAC                                        import gLogger
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping  import getGOCSiteName

from DIRAC.ResourceStatusSystem.Command.Command   import *
from DIRAC.ResourceStatusSystem.Command.knownAPIs import initAPIs
from DIRAC.ResourceStatusSystem.Utilities.Utils   import convertTime

################################################################################
################################################################################

class GOCDBStatus_Command(Command):
  
  __APIs__ = [ 'GOCDBClient' ]
  
  def doCommand(self):
    """ 
    Return getStatus from GOC DB Client
    
    :attr:`args`: 
     - args[0]: string: should be a ValidRes

     - args[1]: string: should be the name of the ValidRes

     - args[2]: string: optional, number of hours in which 
     the down time is starting
    """
    
    timeFormat = "%Y-%m-%d %H:%M"
    
    super(GOCDBStatus_Command, self).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )
    
    granularity = self.args[0]
    name = self.args[1]  
    try:  
      hours = self.args[2]
    except IndexError:
      hours = None

    if granularity in ('Site', 'Sites'):
      name = getGOCSiteName(name)
      if not name['OK']:
        raise RSSException, name['Message']
      name = name['Value']

    try:
      res = self.APIs[ 'GOCDBClient' ].getStatus( granularity, name, None, hours )
      if not res['OK']:
        return {'Result':'Unknown'}
      res = res['Value']
      if res is None or res == {}:
        return {'Result':{'DT':None}}
      
      DT_dict_result = {}
      
      now = datetime.utcnow().replace(microsecond = 0, second = 0)
      
      if len(res) > 1:
        #there's more than one DT
        for dt_ID in res:
          #looking for an ongoing one
          startSTR = res[dt_ID]['FORMATED_START_DATE']
          "%Y-%m-%d %H:%M"
          start_datetime = datetime.strptime( startSTR, timeFormat )
          if start_datetime < now:
            resDT = res[dt_ID]
            break
        try:
          resDT
        except:
          #if I'm here, there's no OnGoing DT
          resDT = res[res.keys()[0]]
        res = resDT
      else:
        res = res[res.keys()[0]]

      DT_dict_result['DT'] = res['SEVERITY']
      DT_dict_result['EndDate'] = res['FORMATED_END_DATE']
      startSTR = res['FORMATED_START_DATE']
      start_datetime = datetime.strptime( startSTR, timeFormat )
      if start_datetime > now:
        diff = convertTime(start_datetime - now, 'hours')
        DT_dict_result['DT'] = DT_dict_result['DT'] + " in " + str(diff) + ' hours'
          
      return {'Result':DT_dict_result}
        
    except urllib2.URLError:
      gLogger.error("GOCDB timed out for " + granularity + " " + name )
      return  {'Result':'Unknown'}      
    except:
      gLogger.exception("Exception when calling GOCDBClient for " + granularity + " " + name )
      return {'Result':'Unknown'}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
################################################################################

class DTCached_Command(Command):
  
  __APIs__ = [ 'ResourceManagementClient' ]
  
  def doCommand(self):
    """ 
    Returns DT Information that are cached.

    :attr:`args`: 
       - args[0]: string: should be a ValidRes
  
       - args[1]: string should be the name of the ValidRes

       - args[2]: string: optional, number of hours in which 
       the down time is starting
    """
    
    timeFormat = "%Y-%m-%d %H:%M"
    
    super(DTCached_Command, self).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )    

    granularity = self.args[0]
    name = self.args[1]

    now = datetime.utcnow().replace(microsecond = 0, second = 0)
    
    try:
      if granularity in ('Site', 'Sites'):
        commandName = 'DTEverySites'
      elif self.args[0] in ('Resource', 'Resources'):
        commandName = 'DTEveryResources'

      res = self.APIs[ 'ResourceManagementClient' ].getClientCache( name = name, commandName = commandName, meta = { 'columns': 'opt_ID' })
      
      if not res['OK']:
        raise RSSException, commandName
      res = res['Value']
      if res is None or len( res ) == 0:
        return {'Result':{'DT':None}}
      
      #CachedResult
      clientDict = { 
                     'name'        : name,
                     'commandName' : commandName,
                     'value'       : None,
                     'opt_ID'      : None,
                     'meta'        : { 'columns' : 'Result' }
                   }
      
      if len( res ) > 1:
        #there's more than one DT
        
        dt_ID_startingSoon = res[0]
        clientDict[ 'value' ]  = 'StartDate'
        clientDict[ 'opt_ID' ] = dt_ID_startingSoon 
        
        startSTR_startingSoon = self.APIs[ 'ResourceManagementClient' ].getClientCache( **clientDict )[ 'Value' ]
        if startSTR_startingSoon:
          startSTR_startingSoon = startSTR_startingSoon[0][0]    
                 
        clientDict[ 'value' ]  = 'EndDate'
        clientDict[ 'opt_ID' ] = dt_ID_startingSoon
        endSTR_startingSoon = self.APIs[ 'ResourceManagementClient' ].getClientCache( **clientDict )[ 'Value' ]            
        if endSTR_startingSoon:
          endSTR_startingSoon = endSTR_startingSoon[0][0]
        start_datetime_startingSoon = datetime.strptime( startSTR_startingSoon, timeFormat )
        
        end_datetime_startingSoon   = datetime.strptime( endSTR_startingSoon, timeFormat )        

        if start_datetime_startingSoon < now:
          if end_datetime_startingSoon > now:
            #ongoing downtime found!
            DT_ID = dt_ID_startingSoon
        
        try:
          DT_ID
        except:
          for dt_ID in res[1:]:
            #looking for an ongoing one
            clientDict[ 'value' ] = 'StartDate'
            clientDict[ 'opt_ID' ] = dt_ID 
            startSTR = self.APIs[ 'ResourceManagementClient' ].getClientCache( **clientDict )[ 'Value' ]
            if startSTR:
              startSTR = startSTR[0][0]
           
            clientDict[ 'value' ] = 'EndDate'
            clientDict[ 'opt_ID' ] = dt_ID 
            endSTR = self.APIs[ 'ResourceManagementClient' ].getClientCache( **clientDict )[ 'Value' ]
            if endSTR:
              endSTR = endSTR[0][0]
            start_datetime = datetime.strptime( startSTR, timeFormat )
            end_datetime   = datetime.strptime( endSTR, timeFormat )

            if start_datetime < now:
              if end_datetime > now:
                #ongoing downtime found!
                DT_ID = dt_ID
              break
            if start_datetime < start_datetime_startingSoon:
              #the DT starts before the former considered one
              dt_ID_startingSoon = dt_ID
          try:
            DT_ID
          except:
            #if I'm here, there's no OnGoing DT
            DT_ID = dt_ID_startingSoon

      else:
        DT_ID = res[0]

      DT_dict_result = {}

      clientDict[ 'value' ] = 'StartDate'
      clientDict[ 'opt_ID' ] = DT_ID 
      startSTR = self.APIs[ 'ResourceManagementClient' ].getClientCache( **clientDict )[ 'Value' ]
      if startSTR:
        startSTR = startSTR[0][0]
        
      clientDict[ 'value' ] = 'EndDate'
      clientDict[ 'opt_ID' ] = DT_ID 
      endSTR = self.APIs[ 'ResourceManagementClient' ].getClientCache( **clientDict )[ 'Value' ]
      if endSTR:
        endSTR = endSTR[0][0]
            
      start_datetime = datetime.strptime(startSTR, timeFormat )
      end_datetime   = datetime.strptime(endSTR, timeFormat )      
      
      if end_datetime < now:
        return {'Result': {'DT':None}}
      
      clientDict[ 'value' ] = 'Severity'
      clientDict[ 'opt_ID' ] = DT_ID 
      
      DT_dict_result['DT'] = self.APIs[ 'ResourceManagementClient' ].getClientCache( **clientDict )[ 'Value' ]
      if DT_dict_result['DT']:
        DT_dict_result['DT'] = DT_dict_result['DT'][0][0]
      DT_dict_result['EndDate'] = endSTR
      
      if start_datetime > now:
        try:
          self.args[2]
          diff = convertTime(start_datetime - now, 'hours')
          if diff > self.args[2]:
            return {'Result': {'DT':None}}
          
          DT_dict_result['DT'] = DT_dict_result['DT'] + " in " + str(diff) + ' hours'
        except:
          # Searching only for onGoing DT, got future ones 
          return {'Result': {'DT':None}}
          
      return {'Result':DT_dict_result}

    except urllib2.URLError:
      gLogger.error("GOCDB timed out for " + self.args[0] + " " + self.args[1] )
      return  {'Result':'Unknown'}      
    except:
      gLogger.exception("Exception when calling GOCDBClient for " + self.args[0] + " " + self.args[1] )
      return {'Result':'Unknown'}
      
  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
################################################################################

class DTInfo_Cached_Command(Command):
  
  __APIs__ = [ 'ResourceManagementClient' ]
  
  def doCommand(self):
    """ 
    Returns DT info that are cached.

    :attr:`args`: 
       - args[0]: string: should be a ValidRes
  
       - args[1]: string should be the name of the ValidRes

       - args[2]: string: optional, number of hours in which 
       the down time is starting
    """
    
    timeFormat = "%Y-%m-%d %H:%M"
    
    super(DTInfo_Cached_Command, self).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )     

    granularity = self.args[0]
    name        = self.args[1]

    now = datetime.utcnow().replace( microsecond = 0, second = 0 )
    
    try:
      if granularity in ('Site', 'Sites'):
        commandName = 'DTEverySites'
      elif self.args[0] in ('Resource', 'Resources'):
        commandName = 'DTEveryResources'

      res = self.APIs[ 'ResourceManagementClient' ].getClientCache( name = name, commandName = commandName, meta = {'columns': 'opt_ID'} )
      
      if res[ 'OK' ]:
        res = res[ 'Value' ]    
      else:
        res = []
      
      #CachedResult
      clientDict = { 
                     'name'        : name,
                     'commandName' : commandName,
                     'value'       : None,
                     'opt_ID'      : None,
                     'meta'        : { 'columns'     : 'Result' }
                   }
       
      if len(res) == 0:
        return {'Result':{'DT':None}}

      if len(res) > 1:
        #there's more than one DT
        
        dt_ID_startingSoon = res[0]
        clientDict[ 'value' ] = 'StartDate'
        clientDict[ 'optID' ] = dt_ID_startingSoon 
        startSTR_startingSoon = self.APIs[ 'ResourceManagementClient' ].getClientCache( **clientDict )[ 'Value' ]
        if startSTR_startingSoon:
          startSTR_startingSoon = startSTR_startingSoon[0][0]
                                                            
        clientDict[ 'value' ] = 'EndDate'
        clientDict[ 'optID' ] = dt_ID_startingSoon 
        endSTR_startingSoon = self.APIs[ 'ResourceManagementClient' ].getClientCache( **clientDict )[ 'Value' ]
        if endSTR_startingSoon:
          endSTR_startingSoon = endSTR_startingSoon[0][0]
        
        start_datetime_startingSoon = datetime.strptime(startSTR_startingSoon,
                                                                timeFormat )
        end_datetime_startingSoon = datetime.strptime(endSTR_startingSoon,
                                                             timeFormat )
        
        if start_datetime_startingSoon < now:
          if end_datetime_startingSoon > now:
            #ongoing downtime found!
            DT_ID = dt_ID_startingSoon
        
        try:
          DT_ID
        except:
          for dt_ID in res[1:]:
            #looking for an ongoing one
            clientDict[ 'value' ] = 'StartDate'
            clientDict[ 'optID' ] = dt_ID 
            startSTR = self.APIs[ 'ResourceManagementClient' ].getClientCache( **clientDict )[ 'Value' ]
            if startSTR:
              startSTR = startSTR[0][0]
            
            clientDict[ 'value' ] = 'EndDate'
            clientDict[ 'optID' ] = dt_ID 
            endSTR = self.APIs[ 'ResourceManagementClient' ].getClientCache( **clientDict )[ 'Value' ]
            if endSTR:
              endSTR = endSTR[0][0]
            
            start_datetime = datetime.strptime( startSTR, timeFormat )
            end_datetime   = datetime.strptime( endSTR, timeFormat )

            if start_datetime < now:
              if end_datetime > now:
                #ongoing downtime found!
                DT_ID = dt_ID
              break
            if start_datetime < start_datetime_startingSoon:
              #the DT starts before the former considered one
              dt_ID_startingSoon = dt_ID
          try:
            DT_ID
          except:
            #if I'm here, there's no OnGoing DT
            DT_ID = dt_ID_startingSoon

      else:
        DT_ID = res[0]

      DT_dict_result = {}

      clientDict[ 'value' ] = 'EndDate'
      clientDict[ 'optID' ] = DT_ID 
      endSTR = self.APIs[ 'ResourceManagementClient' ].getClientCache( **clientDict )[ 'Value' ]
      if endSTR:
        endSTR = endSTR[0][0]
      end_datetime = datetime.strptime( endSTR, timeFormat )
      if end_datetime < now:
        return {'Result': {'DT':None}}
      
      DT_dict_result['EndDate'] = endSTR
      
      clientDict[ 'value' ] = 'Severity'
      clientDict[ 'optID' ] = DT_ID 
      DT_dict_result['DT']  = self.APIs[ 'ResourceManagementClient' ].getClientCache( **clientDict )[ 'Value' ]
      if DT_dict_result['DT']:
        DT_dict_result['DT'] = DT_dict_result['DT'][0][0]
       
      clientDict[ 'value' ] = 'StartDate'
      clientDict[ 'optID' ] = DT_ID 
      DT_dict_result['StartDate'] = self.APIs[ 'ResourceManagementClient' ].getClientCache( **clientDict )[ 'Value' ]
      if DT_dict_result['StartDate']:
        DT_dict_result['StartDate'] = DT_dict_result['StartDate'][0][0] 
      
      clientDict[ 'value' ] = 'Description'
      clientDict[ 'optID' ] = DT_ID 
      DT_dict_result['Description'] = self.APIs[ 'ResourceManagementClient' ].getClientCache( **clientDict )[ 'Value' ]
      if DT_dict_result['Description']:
        DT_dict_result['Description'] = DT_dict_result['Description'][0][0]
      
      clientDict[ 'value' ] = 'Link'
      clientDict[ 'optID' ] = DT_ID 
      DT_dict_result['Link'] = self.APIs[ 'ResourceManagementClient' ].getClientCache( **clientDict )[ 'Value' ]
      if DT_dict_result['Link']:
        DT_dict_result['Link'] = DT_dict_result['Link'][0][0]
      
      start_datetime = datetime.strptime( DT_dict_result['StartDate'], timeFormat )
      
      if start_datetime > now:
        try:
          self.args[2]
          diff = convertTime(start_datetime - now, 'hours')
          if diff > self.args[2]:
            return {'Result': {'DT':None}}
          
          DT_dict_result['DT'] = DT_dict_result['DT'] + " in " + str(diff) + ' hours'
        except:
          # Searching only for onGoing DT, got future ones 
          return {'Result': {'DT':None}}
          
      return {'Result':DT_dict_result}

    except urllib2.URLError:
      gLogger.error("GOCDB timed out for " + self.args[0] + " " + self.args[1] )
      return  {'Result':'Unknown'}      
    except:
      gLogger.exception("Exception when calling GOCDBClient for " + self.args[0] + " " + self.args[1] )
      return {'Result':'Unknown'}
      
  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF