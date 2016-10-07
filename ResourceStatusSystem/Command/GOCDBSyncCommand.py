'''
  GOCDBSyncCommand module
'''

import requests
import xml.dom.minidom as minidom
from DIRAC                                                      import S_OK
from DIRAC.Core.LCG.GOCDBClient                                 import GOCDBClient
from DIRAC.Core.LCG.GOCDBClient                                 import _parseSingleElement
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

__RCSID__ = '$Id:  $'

class CheckStatusCommand( Command ):

  def __init__( self, args = None, clients = None ):

    super( CheckStatusCommand, self ).__init__( args, clients )

    if 'GOCDBClient' in self.apis:
      self.gClient = self.apis[ 'GOCDBClient' ]
    else:
      self.gClient = GOCDBClient()

    if 'ResourceManagementClient' in self.apis:
      self.rmClient = self.apis[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient()

  def doNew( self, masterParams = None ):

    """
    Gets the downtime IDs and dates of a given hostname from the local database and compares the results
    with the remote database of GOCDB. If the downtime dates have been changed it updates the local database.
    """

    if masterParams is not None:
      hostname = masterParams
    else:
      hostname = self._prepareCommand()
      if not hostname[ 'OK' ]:
        return hostname

    rmClient = ResourceManagementClient()

    result = rmClient.selectDowntimeCache( name = hostname )
    if not result[ 'OK' ]:
      return result

    for downtimes in result['Value']:

      localDBdict = { 'DowntimeID': downtimes[3],
                      'FORMATED_START_DATE': downtimes[6].strftime('%Y-%m-%d %H:%M'),
                      'FORMATED_END_DATE': downtimes[7].strftime('%Y-%m-%d %H:%M') }

      r = requests.get('https://goc.egi.eu/gocdbpi_v4/public/?method=get_downtime&topentity=' + hostname, verify=False)
      doc = minidom.parseString( r.text )
      downtimeElements = doc.getElementsByTagName( "DOWNTIME" )

      for dtElement in downtimeElements:
        GOCDBdict = _parseSingleElement( dtElement, [ 'PRIMARY_KEY', 'ENDPOINT',
                                                      'FORMATED_START_DATE', 'FORMATED_END_DATE' ] )

        localDowntimeID = localDBdict['DowntimeID']
        GOCDBDowntimeID = GOCDBdict['PRIMARY_KEY'] + ' ' + GOCDBdict['ENDPOINT']

        if localDowntimeID == GOCDBDowntimeID:

          if localDBdict['FORMATED_START_DATE'] != GOCDBdict['FORMATED_START_DATE']:
            result = rmClient.addOrModifyDowntimeCache( downtimeID = localDBdict['DowntimeID'],
                                                        startDate = GOCDBdict['FORMATED_START_DATE'])
            if not result[ 'OK' ]:
              return result

          if localDBdict['FORMATED_END_DATE'] != GOCDBdict['FORMATED_END_DATE']:
            result = rmClient.addOrModifyDowntimeCache( downtimeID = localDBdict['DowntimeID'],
                                                        endDate = GOCDBdict['FORMATED_END_DATE'] )

            if not result[ 'OK' ]:
              return result

    return S_OK()