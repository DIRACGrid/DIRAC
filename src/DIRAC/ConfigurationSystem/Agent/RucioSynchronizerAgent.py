""" :mod: RucioSynchronizer

  Agent that synchronizes Rucio and Dirac

The following options can be set for the RucioSynchronizerAgent.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN RucioSynchronizerAgent
  :end-before: ##END
  :dedent: 2
  :caption: RucioSynchronizerAgent options
"""

# # imports
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from itertools import permutations
from traceback import format_exc

from rucio.client import Client
from rucio.common.exception import RSEProtocolNotSupported, Duplicate, RSEAttributeNotFound

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import (getUserOption, getAllUsers, getHosts,
                                                               getHostOption, getAllGroups, getDNsInGroup)
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers, resolveSEGroup
from DIRAC.Resources.Storage.StorageElement import StorageElement


__RCSID__ = "Id$"


def getStorageElements():
  """
  Get configuration of storage elements

  :return: S_OK/S_ERROR, Value dictionary with key SE and value protocol list
  """
  seProtocols = {}
  dms = DMSHelpers()
  for seName in dms.getStorageElements():
    seProtocols[seName] = []
    se = StorageElement(seName)
    all_protocols = []

    read_protocols = {}
    protocols = se.options.get('AccessProtocols')
    if not protocols:
      continue
    idx = 1
    for prot in protocols:
      read_protocols[prot] = idx
      idx += 1
      if prot not in all_protocols:
        all_protocols.append(prot)
    write_protocols = {}
    protocols = se.options.get('WriteProtocols')
    if not protocols:
      continue
    idx = 1
    for prot in protocols:
      write_protocols[prot] = idx
      idx += 1
      if prot not in all_protocols:
        all_protocols.append(prot)

    mapping = {'Protocol': 'scheme', 'Host': 'hostname', 'Port': 'port', 'Path': 'prefix'}
    for protocol in all_protocols:
      space_token = None
      params = {'hostname': None,
                'scheme': None,
                'port': None,
                'prefix': None,
                'impl': 'rucio.rse.protocols.gfal.Default',
                'domains': {"lan": {"read": 0,
                                    "write": 0,
                                    "delete": 0},
                            "wan": {"read": 0,
                                    "write": 0,
                                    "delete": 0,
                                    "third_party_copy": 0}}}
      res = se.getStorageParameters(protocol=protocol)
      if res['OK']:
        values = res['Value']
        for key in ['Protocol', 'Host', 'Access', 'Path', 'Port', 'WSUrl', 'SpaceToken', 'WSUrl', 'PluginName']:
          value = values.get(key)
          if key in mapping:
            params[mapping[key]] = value
          else:
            if key == 'SpaceToken':
              space_token = value
            if params['scheme'] == 'srm' and key == 'WSUrl':
              params['extended_attributes'] = {'web_service_path': '%s' % value, 'space_token': space_token}
          if key == 'Protocol':
            params['domains']['lan']['read'] = read_protocols.get(value, 0)
            params['domains']['wan']['read'] = read_protocols.get(value, 0)
            params['domains']['lan']['write'] = write_protocols.get(value, 0)
            params['domains']['wan']['write'] = write_protocols.get(value, 0)
            params['domains']['lan']['delete'] = write_protocols.get(value, 0)
            params['domains']['wan']['delete'] = write_protocols.get(value, 0)
            params['domains']['wan']['third_party_copy'] = write_protocols.get(value, 0)
        seProtocols[seName].append(params)
  return S_OK(seProtocols)


class RucioSynchronizerAgent(AgentModule):
  """
  .. class::  RucioSynchronizerAgent

  Agent that synchronizes Rucio and Dirac
  """

  def execute(self):
    """ execution in one agent's cycle

    :param self: self reference
    """

    valid_protocols = ['srm', 'gsiftp', 'davs', 'https', 'root']
    default_email = None
    try:
      client = Client(account='root', auth_type='userpass')

      # Get the storage elements from Dirac Configuration and create them in Rucio
      newRSE = False
      self.log.info("Synchronizing SEs")
      result = getStorageElements()
      if result['OK']:
        rses = [rse['rse'] for rse in client.list_rses()]
        for se in result['Value']:
          if se not in rses:
            # The SE doesn't exist. Will create it
            newRSE = True
            self.log.info('%s does not exist and will be created' % se)
            try:
              client.add_rse(rse=se, deterministic=True, volatile=False)
            except Exception as err:
              self.log.error('Cannot create RSE' % str(err))
              continue

            # Add RSE attributes for the new RSE
            ret = gConfig.getOptionsDict('Resources/FTSEndpoints/FTS3')
            ftsList = ''
            if ret['OK']:
              ftsList = ",".join(ret['Value'].values())
            dictRSEAttributes = {'naming_convention': 'BelleII',
                                 'ANY': True,
                                 'fts': ftsList}
            for key in dictRSEAttributes:
              self.log.info('On %s, setting %s : %s', se, key, dictRSEAttributes[key])
              client.add_rse_attribute(se, key, value=dictRSEAttributes[key])
            client.set_local_account_limit('root', se, 100000000000000000)

          # Create the protocols
          try:
            protocols = client.get_protocols(se)
          except RSEProtocolNotSupported as err:
            self.log.info('Cannot get protocols for %s : %s', se, str(err))
            protocols = []
          existing_protocols = []
          for prot in protocols:
            existing_protocols.append((str(prot['scheme']),
                                       str(prot['hostname']),
                                       str(prot['port']),
                                       str(prot['prefix'])))
          protocols_to_create = []
          for params in result['Value'][se]:
            prot = (str(params['scheme']), str(params['hostname']), str(params['port']), str(params['prefix']))
            protocols_to_create.append(prot)
            if prot not in existing_protocols and prot[0] in valid_protocols:
              # The protocol defined in Dirac does not exist in Rucio. Will be created
              self.log.info('Will create new protocol %s://%s:%s%s on %s', params['scheme'],
                            params['hostname'],
                            params['port'],
                            params['prefix'],
                            se)
              try:
                client.add_protocol(rse=se, params=params)
              except Duplicate:
                self.log.info('Protocol %s already exists on %s', params['scheme'], se)
              except Exception as err:
                self.log.error('Cannot create protocol on RSE %s : %s', se, str(err))
            else:
              update = False
              for protocol in protocols:
                if prot == (str(protocol['scheme']),
                            str(protocol['hostname']),
                            str(protocol['port']),
                            str(protocol['prefix'])):
                  # Check if the protocol defined in Dirac has the same priority as the one defined in Rucio
                  for domain in ['lan', 'wan']:
                    for activity in ['read', 'write', 'delete']:
                      if params['domains'][domain][activity] != protocol['domains'][domain][activity]:
                        update = True
                        break

                  if params['domains']['wan']['third_party_copy'] != protocol['domains']['wan']['third_party_copy']:
                    update = True
                  if update:
                    data = {'prefix': params['prefix'],
                            'read_lan': params['domains']['lan']['read'],
                            'read_wan': params['domains']['wan']['read'],
                            'write_lan': params['domains']['lan']['write'],
                            'write_wan': params['domains']['wan']['write'],
                            'delete_lan': params['domains']['lan']['delete'],
                            'delete_wan': params['domains']['wan']['delete'],
                            'third_party_copy': params['domains']['wan']['write']}
                    self.log.info('Will update protocol %s://%s:%s%s on %s', params['scheme'],
                                  params['hostname'],
                                  params['port'],
                                  params['prefix'],
                                  se)
                    client.update_protocols(rse=se,
                                            scheme=params['scheme'],
                                            data=data,
                                            hostname=params['hostname'],
                                            port=params['port'])
          for prot in existing_protocols:
            if prot not in protocols_to_create:
              self.log.info('Will delete protocol %s://%s:%s%s on %s', prot[0], prot[1], prot[2], prot[3], se)
              client.delete_protocols(se, scheme=prot[0], hostname=prot[1], port=prot[2])
      else:
        self.log.error('Cannot get SEs : %s', result['Value'])

      # If new RSE added, add distances
      rses = [rse['rse'] for rse in client.list_rses()]
      if newRSE:
        self.log.info("Adding distances")
        for src_rse, dest_rse in permutations(rses, r=2):
          try:
            client.add_distance(src_rse, dest_rse, {'ranking': 1, 'distance': 10})
          except Exception as err:
            self.log.error('Cannot add distance for %s:%s : %s', src_rse, dest_rse, str(err))

      # Collect the shares from Dirac Configuration and create them in Rucio
      self.log.info("Synchronizing shares")
      result = Operations().getOptionsDict('Production/SEshares')
      if result['OK']:
        rseDict = result['Value']
        for rse in rses:
          try:
            self.log.info('Setting productionSEshare for %s : %s', rse, rseDict.get(rse, 0))
            client.add_rse_attribute(rse, 'productionSEshare', rseDict.get(rse, 0))
          except Exception:
            self.log.error('Cannot create productionSEshare for %s', rse)
      else:
        self.log.error('Cannot get SEs : %s', result['Value'])

      result = Operations().getSections('Shares')
      if result['OK']:
        for dataLevel in result['Value']:
          result = Operations().getOptionsDict('Shares/%s' % dataLevel)
          if not result['OK']:
            self.log.error('Cannot get SEs : %s', result['Value'])
            continue
          rseDict = result['Value']
          for rse in rses:
            try:
              self.log.info('Setting %sShare for %s : %s', dataLevel, rse, rseDict.get(rse, 0))
              client.add_rse_attribute(rse, '%sShare' % dataLevel, rseDict.get(rse, 0))
            except Exception:
              self.log.error('Cannot create %sShare for %s', dataLevel, rse)
      else:
        self.log.error('Cannot get shares : %s', result['Value'])

      # Create the RSE attribute PrimaryDataSE and OccupancyLFN
      result = gConfig.getValue('Resources/StorageElementGroups/PrimarySEs')
      result = getStorageElements()
      if result['OK']:
        allSEs = result['Value']
        primarySEs = resolveSEGroup('PrimarySEs', allSEs)
        self.log.info('Will set primarySEs flag to %s', str(primarySEs))
        for rse in rses:
          if rse in allSEs:
            occupancyLFN = StorageElement(rse).options.get('OccupancyLFN')
            try:
              client.add_rse_attribute(rse, 'OccupancyLFN', occupancyLFN)
            except Exception as err:
              self.log.error('Cannot create RSE attribute OccupancyLFN for %s : %s', rse, str(err))
          if rse in primarySEs:
            try:
              client.add_rse_attribute(rse, 'PrimaryDataSE', True)
            except Exception as err:
              self.log.error('Cannot create RSE attribute PrimaryDataSE for %s : %s', rse, str(err))
          else:
            try:
              client.delete_rse_attribute(rse, 'PrimaryDataSE')
            except RSEAttributeNotFound:
              pass
            except Exception as err:
              self.log.error('Cannot remove RSE attribute PrimaryDataSE for %s : %s', rse, str(err))

      # Collect the user accounts from Dirac Configuration and create user accounts in Rucio
      self.log.info("Synchronizing accounts")
      listAccounts = [str(acc['account']) for acc in client.list_accounts()]
      listScopes = [str(scope) for scope in client.list_scopes()]
      dnMapping = {}
      for account in getAllUsers():
        dn = getUserOption(account, 'DN')
        email = getUserOption(account, 'Email')
        dnMapping[dn] = email
        if account not in listAccounts:
          self.log.info('Will create %s with associated DN %s', account, dn)
          try:
            client.add_account(account=account, type='USER', email=email)
            listAccounts.append(account)
          except Exception as err:
            self.log.error('Cannot create account %s : %s', account, str(err))
          try:
            client.add_identity(account=account, identity=dn, authtype='X509', email=email, default=True)
          except Exception as err:
            self.log.error('Cannot create account %s : %s', account, str(err))
          for rse in rses:
            client.set_local_account_limit(account, rse, 1000000000000000)
        else:
          try:
            client.add_identity(account=account, identity=dn, authtype='X509', email=email, default=True)
          except Duplicate:
            pass
          except Exception as err:
            self.log.error('Cannot create identity %s for account %s : %s', dn, account, str(err))
        scope = 'user.' + account
        if scope not in listScopes:
          try:
            self.log.info('Will create scope %s', scope)
            client.add_scope(account, scope)
            self.log.info('Scope %s successfully added', scope)
          except Exception as err:
            self.log.error('Cannot create scope %s : %s', scope, str(err))

      # Collect the group accounts from Dirac Configuration and create service accounts in Rucio
      groups = getAllGroups()
      for group in groups:
        if group not in listAccounts:
          self.log.info('Will create SERVICE account %s' % (group))
          try:
            client.add_account(account=group, type='SERVICE', email=None)
            listAccounts.append(group)
          except Exception as err:
            self.log.error('Cannot create account %s : %s' % (group, str(err)))
          for rse in rses:
            client.set_local_account_limit(account, rse, 1000000000000000)

        for dn in getDNsInGroup(group):
          try:
            client.add_identity(account=group, identity=dn, authtype='X509', email=dnMapping.get(dn, default_email))
          except Duplicate:
            pass
          except Exception as err:
            self.log.error('Cannot create identity %s for account %s : %s' % (dn, group, str(err)))
            self.log.error(format_exc())

      # Collect the group accounts from Dirac Configuration and create service accounts in Rucio
      result = getHosts()
      if not result['OK']:
        self.log.error('Cannot get host accounts : %s' % result['Value'])
      else:
        hosts = result['Value']
        for host in hosts:
          dn = getHostOption(host, 'DN')
          email = dnMapping.get(dn, default_email)
          try:
            client.add_identity(account='dirac_srv', identity=dn, authtype='X509', email=email)
          except Duplicate:
            pass
          except Exception as err:
            self.log.error('Cannot create identity %s for account dirac_srv : %s' % (dn, str(err)))
            self.log.error(format_exc())

      return S_OK()
    except Exception:
      return S_ERROR(str(format_exc()))
