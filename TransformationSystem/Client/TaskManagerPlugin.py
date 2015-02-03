""" Container for TaskManager plug-ins, to handle the destination of the tasks
"""

from DIRAC import S_OK, S_ERROR

from DIRAC.Core.Utilities.List import fromChar
from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE
from DIRAC.TransformationSystem.Client.Plugins import Plugins


class TaskManagerPlugin( Plugins ):
  """ A TaskManagerPlugin object should be instantiated by every TaskManager object.
  
      self.params here could be 
      {'Status': 'Created', 'TargetSE': 'Unknown', 'TransformationID': 1086L, 'RunNumber': 0L, 
      'Site': 'DIRAC.Test.ch', 'TaskID': 21L, 'InputData': '', 'JobType': 'MCSimulation'}
      which corresponds to paramsDict in TaskManager (which is in fact a tasks dict)
  """

  def _BySE( self ):
    """ Matches using TargetSE. This is the standard plugin.
    """

    destSites = set()

    try:
      seList = ['Unknown']
      if self.params['TargetSE']:
        if type( self.params['TargetSE'] ) == type( '' ):
          seList = fromChar( self.params['TargetSE'] )
        elif type( self.params['TargetSE'] ) == type( [] ):
          seList = self.params['TargetSE']
    except KeyError:
      pass

    if not seList or seList == ['Unknown']:
      return destSites
    
    for se in seList:
      res = getSitesForSE( se )
      if not res['OK']:
        self.log.warn( "Could not get Sites associated to SE", res['Message'] )
      else:
        thisSESites = res['Value']
        if thisSESites:
          # We make an OR of the possible sites
          destSites.update( thisSESites )
          
    return destSites
    

  def _ByJobType( self ):
    """ Looks in ActivityMapping section

        The section may look like this:

        ActivityMapping
        {
          User
          {
            Exclude = PAK
            Exclude += Ferrara
            Exclude += Bologna
            Exclude += Paris
            Exclude += CERN
            Exclude += IN2P3
            Allow = Paris <- IN2P3
            Allow += CERN <- CERN
            Allow += IN2P3 <- IN2P3
          }
          Merge
          {
            Exclude = ALL
            Allow = CERN <- CERN
            Allow += IN2P3 <- IN2P3
          }
        }
        
        Then it will look into...
        
        
    """
    jobType = self.params['JobType']
    if not jobType:
      return S_ERROR( "No jobType specified" )
    res = self.opsH.getOptionsDict( 'JobTypeMapping/%s/' % jobType )
    if not res['OK']:
      self.log.error( "Could not get mapping by job type", res['Message'] )
      return res
    jobTypeMapping = res['Value']
    # FIXME: here we should add the obvious relation in allow
    return S_OK( jobTypeMapping )

