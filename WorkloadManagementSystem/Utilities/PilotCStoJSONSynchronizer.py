""" CStoJSONSynchronizer

  Module that keeps the pilot parameters file synchronized with the information
  in the Operations/Pilot section of the CS. If there are additions in the CS,
  these are incorporated to the file.
  The module uploads to a web server the latest version of the pilot scripts.

"""

import json
import urllib
import shutil
import os
import glob
import tarfile
from git import Repo

from DIRAC                                    import gLogger, S_OK, gConfig, S_ERROR
from DIRAC.Core.DISET.HTTPDISETConnection     import HTTPDISETConnection

__RCSID__ = '$Id:  $'


class PilotCStoJSONSynchronizer( object ):
  '''
  2 functions are executed:
  - It updates a JSON file with the values on the CS which can be used by Pilot3 pilots
  - It updates the pilot 3 files

  This synchronizer can be triggered at any time via PilotCStoJSONSynchronizer().sync().
  As it is today, this is triggered every time there is a successful write on the CS.
  '''

  def __init__( self, paramDict ):
    ''' c'tor

        Just setting defaults
    '''
    self.jsonFile = 'pilot.json'  # default filename of the pilot json file
    # domain name of the web server used to upload the pilot json file and the pilot scripts
    self.pilotFileServer = paramDict['pilotFileServer']
    self.pilotRepo = paramDict['pilotRepo']  # repository of the pilot
    self.pilotVORepo = paramDict['pilotVORepo']  # repository of the VO that can contain a pilot extension
    self.pilotLocalRepo = 'pilotLocalRepo'  # local repository to be created
    self.pilotVOLocalRepo = 'pilotVOLocalRepo'  # local VO repository to be created
    self.pilotSetup = gConfig.getValue( '/DIRAC/Setup', '' )
    self.projectDir = paramDict['projectDir']
    # where the find the pilot scripts in the VO pilot repository
    self.pilotVOScriptPath = paramDict['pilotVOScriptPath']
    self.pilotScriptsPath = paramDict['pilotScriptsPath'] # where the find the pilot scripts in the pilot repository
    self.pilotVersion = ''
    self.pilotVOVersion =''

  def sync( self ):
    ''' Main synchronizer method.
    '''
    gLogger.notice( '-- Synchronizing the content of the JSON file %s with the content of the CS --' % self.jsonFile )

    result = self._syncFile()
    if not result['OK']:
      gLogger.error( "Error uploading the pilot file: %s" % result['Message'] )
      return result

    gLogger.notice( '-- Synchronizing the pilot scripts %s with the content of the repository --' % self.pilotRepo )

    self._syncScripts()

    return S_OK()

  def _syncFile( self ):
    ''' Creates the pilot dictionary from the CS, ready for encoding as JSON
    '''
    pilotDict = self._getCSDict()

    result = self._upload( pilotDict = pilotDict )
    if not result['OK']:
      gLogger.error( "Error uploading the pilot file: %s" %result['Message'] )
      return result
    return S_OK()

  def _getCSDict(self):
    """ Gets minimal info for running a pilot, from the CS

    :returns: pilotDict (containing pilots run info)
    :rtype: dict
    """

    pilotDict = { 'Setups' : {}, 'CEs' : {} }

    gLogger.info( '-- Getting the content of the CS --' )

    # These are in fact not only setups: they may be "Defaults" sections, or VOs, in multi-VOs installations
    setups = gConfig.getSections( '/Operations/' )
    if not setups['OK']:
      gLogger.error( setups['Message'] )
      return setups
    setups = setups['Value']

    try:
      setups.remove( 'SoftwareDistribution' ) #TODO: remove this section
    except (AttributeError, ValueError):
      pass

    # Something inside? (for multi-VO setups)
    for vo in setups:
      setupsFromVOs = gConfig.getSections( '/Operations/%s' % vo )
      if not setupsFromVOs['OK']:
        continue
      else:
        setups.append("%s/%s" %(vo, setupsFromVOs))


    gLogger.verbose( 'From Operations/[Setup]/Pilot' )

    for setup in setups:
      self._getPilotOptionsPerSetup(setup, pilotDict)

    gLogger.verbose( 'From Resources/Sites' )
    sitesSection = gConfig.getSections( '/Resources/Sites/' )
    if not sitesSection['OK']:
      gLogger.error( sitesSection['Message'] )
      return sitesSection

    for grid in sitesSection['Value']:
      gridSection = gConfig.getSections( '/Resources/Sites/' + grid )
      if not gridSection['OK']:
        gLogger.error( gridSection['Message'] )
        return gridSection

      for site in gridSection['Value']:
        ceList = gConfig.getSections( '/Resources/Sites/' + grid + '/' + site + '/CEs/' )
        if not ceList['OK']:
          # Skip but log it
          gLogger.error( 'Site ' + site + ' has no CEs! - skipping' )
          continue

        for ce in ceList['Value']:
          ceType = gConfig.getValue( '/Resources/Sites/' + grid + '/' + site + '/CEs/' + ce + '/CEType')

          if ceType is None:
            # Skip but log it
            gLogger.error( 'CE ' + ce + ' at ' + site + ' has no option CEType! - skipping' )
          else:
            pilotDict['CEs'][ce] = { 'Site' : site, 'GridCEType' : ceType }

    defaultSetup = gConfig.getValue( '/DIRAC/DefaultSetup' )
    if defaultSetup:
      pilotDict['DefaultSetup'] = defaultSetup

    gLogger.verbose( 'From DIRAC/Configuration' )
    pilotDict['ConfigurationServers'] = gConfig.getServersList()

    gLogger.verbose( "Got %s"  %str(pilotDict) )

    return pilotDict


  def _getPilotOptionsPerSetup(self, setup, pilotDict):
    """ Given a setup, returns its pilot options in a dictionary
    """

    options = gConfig.getOptionsDict( '/Operations/%s/Pilot' % setup )
    if not options['OK']:
      gLogger.warn( "Section /Operations/%s/Pilot does not exist: skipping" % setup )
      return

    # We include everything that's in the Pilot section for this setup
    if setup == self.pilotSetup:
      self.pilotVOVersion = options['Value']['Version']
    pilotDict['Setups'][setup] = options['Value']
    ceTypesCommands = gConfig.getOptionsDict( '/Operations/%s/Pilot/Commands' % setup )
    if ceTypesCommands['OK']:
      # It's ok if the Pilot section doesn't list any Commands too
      pilotDict['Setups'][setup]['Commands'] = {}
      for ceType in ceTypesCommands['Value']:
        # FIXME: inconsistent that we break Commands down into a proper list but other things are comma-list strings
        pilotDict['Setups'][setup]['Commands'][ceType] = ceTypesCommands['Value'][ceType].split(', ')
        # pilotDict['Setups'][setup]['Commands'][ceType] = ceTypesCommands['Value'][ceType]
    if 'CommandExtensions' in pilotDict['Setups'][setup]:
      # FIXME: inconsistent that we break CommandExtensionss down into a proper list but other things are comma-list strings
      pilotDict['Setups'][setup]['CommandExtensions'] = pilotDict['Setups'][setup]['CommandExtensions'].split(', ')
      # pilotDict['Setups'][setup]['CommandExtensions'] = pilotDict['Setups'][setup]['CommandExtensions']

    # Getting the details aboout the MQ Services to be used for logging, if any
    if 'LoggingMQService' in pilotDict['Setups'][setup]:
      loggingMQService = gConfig.getOptionsDict( '/Resources/MQServices/%s' \
                                                  % pilotDict['Setups'][setup]['LoggingMQService'])
      if not loggingMQService['OK']:
        gLogger.error( loggingMQService['Message'] )
        return loggingMQService
      pilotDict['Setups'][setup]['Logging'] = {}
      pilotDict['Setups'][setup]['Logging']['Host'] = loggingMQService['Value']['Host']
      pilotDict['Setups'][setup]['Logging']['Port'] = loggingMQService['Value']['Port']

      loggingMQServiceQueuesSections = gConfig.getSections( '/Resources/MQServices/%s/Queues' \
                                                            % pilotDict['Setups'][setup]['LoggingMQService'])
      if not loggingMQServiceQueuesSections['OK']:
        gLogger.error( loggingMQServiceQueuesSections['Message'] )
        return loggingMQServiceQueuesSections
      pilotDict['Setups'][setup]['Logging']['Queue'] = {}
      
      for queue in loggingMQServiceQueuesSections['Value']:
        loggingMQServiceQueue = gConfig.getOptionsDict( '/Resources/MQServices/%s/Queues/%s' \
                                                        % (pilotDict['Setups'][setup]['LoggingMQService'], queue) )
        if not loggingMQServiceQueue['OK']:
          gLogger.error( loggingMQServiceQueue['Message'] )
          return loggingMQServiceQueue
        pilotDict['Setups'][setup]['Logging']['Queue'][queue] = loggingMQServiceQueue['Value']

      pilotDict['Setups'][setup]['Logging']['Queues'] = loggingMQService['Value']['Queues']


  def _syncScripts(self):
    """Clone the pilot scripts from the repository and upload them to the web server
    """
    gLogger.info( '-- Uploading the pilot scripts --' )
    if os.path.isdir( self.pilotVOLocalRepo ):
      shutil.rmtree( self.pilotVOLocalRepo )
    os.mkdir( self.pilotVOLocalRepo )
    repo_VO = Repo.init( self.pilotVOLocalRepo )
    upstream = repo_VO.create_remote( 'upstream', self.pilotVORepo )
    upstream.fetch()
    upstream.pull( upstream.refs[0].remote_head )
    if repo_VO.tags:
      repo_VO.git.checkout( repo_VO.tags[self.pilotVOVersion], b = 'pilotScripts' )
    else:
      repo_VO.git.checkout( 'upstream/master', b = 'pilotVOScripts' )
    scriptDir = ( os.path.join( self.pilotVOLocalRepo, self.projectDir, self.pilotVOScriptPath, "*.py" ) )
    tarFiles = []
    for fileVO in glob.glob( scriptDir ):
      result = self._upload( filename = os.path.basename( fileVO ), pilotScript = fileVO )
      tarFiles.append(fileVO)
    if not result['OK']:
      gLogger.error( "Error uploading the VO pilot script: %s" % result['Message'] )
      return result
    if os.path.isdir( self.pilotLocalRepo ):
      shutil.rmtree( self.pilotLocalRepo )
    os.mkdir( self.pilotLocalRepo )
    repo = Repo.init( self.pilotLocalRepo )
    releases = repo.create_remote( 'releases', self.pilotRepo )
    releases.fetch()
    releases.pull( releases.refs[0].remote_head )
    if repo.tags:
      with open( os.path.join( self.pilotVOLocalRepo, self.projectDir, 'releases.cfg' ), 'r' ) as releases_file:
        lines = [line.rstrip( '\n' ) for line in releases_file]
        lines = [s.strip() for s in lines]
        if self.pilotVOVersion in lines:
          self.pilotVersion = lines[( lines.index( self.pilotVOVersion ) ) + 3].split( ':' )[1]
      repo.git.checkout( repo.tags[self.pilotVersion], b = 'pilotScripts' )
    else:
      repo.git.checkout( 'master', b = 'pilotVOScripts' )
    try:
      scriptDir = os.path.join( self.pilotLocalRepo, self.pilotScriptsPath, "*.py" )
      for filename in glob.glob( scriptDir ):
        result = self._upload(filename = os.path.basename(filename),
                              pilotScript = filename)
        tarFiles.append(filename)
      if not os.path.isfile(os.path.join(self.pilotLocalRepo,
                                         self.pilotScriptsPath,
                                         "dirac-install.py")):
        result = self._upload(filename = 'dirac-install.py',
                              pilotScript = os.path.join( self.pilotLocalRepo, "Core/scripts/dirac-install.py"))
        tarFiles.append('dirac-install.py')

      with tarfile.TarFile(name = 'pilot.tar', mode = 'w') as tf:
        for ptf in tarFiles:
          tf.add(ptf)
      result = self._upload(filename = 'pilot.tar',
                            pilotScript = os.path.join( self.pilotLocalRepo, 'pilot.tar'))

    except ValueError:
      gLogger.error( "Error uploading the pilot scripts: %s" % result['Message'] )
      return result
    return S_OK()


  def _upload ( self, pilotDict = None, filename = '', pilotScript = '' ):
    """ Method to upload the pilot json file and the pilot scripts to the server.
    """

    if pilotDict:
      params = urllib.urlencode( {'filename':self.jsonFile, 'data':json.dumps( pilotDict ) } )
    else:
      with open( pilotScript, "rb" ) as psf:
        script = psf.read()
      params = urllib.urlencode( {'filename':filename, 'data':script} )
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    con = HTTPDISETConnection( self.pilotFileServer, '443' )
    con.request( "POST", "/DIRAC/upload", params, headers )
    resp = con.getresponse()
    if resp.status != 200:
      return S_ERROR( resp.status )
    else:
      gLogger.info( '-- File and scripts upload done --' )
    return S_OK()
