''' AccountingCacheCommand

  The AccountingCacheCommand class is a command module that collects command
  classes to store accounting results in the accounting cache.

'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# FIXME: NOT Usable ATM
# missing doNew, doCache, doMaster

__RCSID__ = '$Id$'

from datetime import datetime, timedelta

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.JEncode import strToIntDict
from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSites, getCESiteMapping
from DIRAC.ResourceStatusSystem.Command.Command import Command


class SuccessfullJobsBySiteSplittedCommand(Command):

  def __init__(self, args=None, clients=None):

    super(SuccessfullJobsBySiteSplittedCommand, self).__init__(args, clients)

    if 'ReportsClient' in self.apis:
      self.rClient = self.apis['ReportsClient']
    else:
      self.rClient = ReportsClient()

  def doCommand(self):
    """
    Returns successfull jobs using the DIRAC accounting system for every site
    for the last self.args[0] hours

    :params:
      :attr:`sites`: list of sites (when not given, take every site)

    :returns:

    """

    if 'hours' not in self.args:
      return S_ERROR('Number of hours not specified')
    hours = self.args['hours']

    sites = None
    if 'sites' in self.args:
      sites = self.args['sites']
    if sites is None:
      sites = getSites()
      if not sites['OK']:
        return sites
      sites = sites['Value']

    if not sites:
      return S_ERROR('Sites is empty')

    fromD = datetime.utcnow() - timedelta(hours=hours)
    toD = datetime.utcnow()

    successfulJobs = self.rClient.getReport('Job', 'NumberOfJobs', fromD, toD,
                                            {'FinalStatus': ['Done'],
                                             'Site': sites}, 'Site')
    if not successfulJobs['OK']:
      return successfulJobs
    successfulJobs = successfulJobs['Value']

    if 'data' not in successfulJobs:
      return S_ERROR('Missing data key')
    if 'granularity' not in successfulJobs:
      return S_ERROR('Missing granularity key')

    singlePlots = {}

    successfulJobs['data'] = {site: strToIntDict(value) for site, value in successfulJobs['data'].items()}

    for site, value in successfulJobs['data'].items():
      if site in sites:
        plot = {}
        plot['data'] = {site: value}
        plot['granularity'] = successfulJobs['granularity']
        singlePlots[site] = plot

    return S_OK(singlePlots)

################################################################################
################################################################################


class FailedJobsBySiteSplittedCommand(Command):

  def __init__(self, args=None, clients=None):

    super(FailedJobsBySiteSplittedCommand, self).__init__(args, clients)

    if 'ReportsClient' in self.apis:
      self.rClient = self.apis['ReportsClient']
    else:
      self.rClient = ReportsClient()

  def doCommand(self):
    """
    Returns failed jobs using the DIRAC accounting system for every site
    for the last self.args[0] hours

    :params:
      :attr:`sites`: list of sites (when not given, take every site)

    :returns:

    """

    if 'hours' not in self.args:
      return S_ERROR('Number of hours not specified')
    hours = self.args['hours']

    sites = None
    if 'sites' in self.args:
      sites = self.args['sites']
    if sites is None:
      # FIXME: pointing to the CSHelper instead
      #      sources = self.rsClient.getSite( meta = {'columns': 'SiteName'} )
      #      if not sources[ 'OK' ]:
      #        return sources
      #      sources = [ si[0] for si in sources[ 'Value' ] ]
      sites = getSites()
      if not sites['OK']:
        return sites
      sites = sites['Value']

    if not sites:
      return S_ERROR('Sites is empty')

    fromD = datetime.utcnow() - timedelta(hours=hours)
    toD = datetime.utcnow()

    failedJobs = self.rClient.getReport('Job', 'NumberOfJobs', fromD, toD,
                                        {'FinalStatus': ['Failed'],
                                         'Site': sites},
                                        'Site')
    if not failedJobs['OK']:
      return failedJobs
    failedJobs = failedJobs['Value']

    if 'data' not in failedJobs:
      return S_ERROR('Missing data key')
    if 'granularity' not in failedJobs:
      return S_ERROR('Missing granularity key')

    failedJobs['data'] = {site: strToIntDict(value) for site, value in failedJobs['data'].items()}

    singlePlots = {}

    for site, value in failedJobs['data'].items():
      if site in sites:
        plot = {}
        plot['data'] = {site: value}
        plot['granularity'] = failedJobs['granularity']
        singlePlots[site] = plot

    return S_OK(singlePlots)

################################################################################
################################################################################


class SuccessfullPilotsBySiteSplittedCommand(Command):

  def __init__(self, args=None, clients=None):

    super(SuccessfullPilotsBySiteSplittedCommand, self).__init__(args, clients)

    if 'ReportsClient' in self.apis:
      self.rClient = self.apis['ReportsClient']
    else:
      self.rClient = ReportsClient()

  def doCommand(self):
    """
    Returns successfull pilots using the DIRAC accounting system for every site
    for the last self.args[0] hours

    :params:
      :attr:`sites`: list of sites (when not given, take every site)

    :returns:

    """

    if 'hours' not in self.args:
      return S_ERROR('Number of hours not specified')
    hours = self.args['hours']

    sites = None
    if 'sites' in self.args:
      sites = self.args['sites']
    if sites is None:
      # FIXME: pointing to the CSHelper instead
      #      sources = self.rsClient.getSite( meta = {'columns': 'SiteName'} )
      #      if not sources[ 'OK' ]:
      #        return sources
      #      sources = [ si[0] for si in sources[ 'Value' ] ]
      sites = getSites()
      if not sites['OK']:
        return sites
      sites = sites['Value']

    if not sites:
      return S_ERROR('Sites is empty')

    fromD = datetime.utcnow() - timedelta(hours=hours)
    toD = datetime.utcnow()

    succesfulPilots = self.rClient.getReport('Pilot', 'NumberOfPilots', fromD, toD,
                                             {'GridStatus': ['Done'],
                                              'Site': sites},
                                             'Site')
    if not succesfulPilots['OK']:
      return succesfulPilots
    succesfulPilots = succesfulPilots['Value']

    if 'data' not in succesfulPilots:
      return S_ERROR('Missing data key')
    if 'granularity' not in succesfulPilots:
      return S_ERROR('Missing granularity key')

    succesfulPilots['data'] = {site: strToIntDict(value) for site, value in succesfulPilots['data'].items()}

    singlePlots = {}

    for site, value in succesfulPilots['data'].items():
      if site in sites:
        plot = {}
        plot['data'] = {site: value}
        plot['granularity'] = succesfulPilots['granularity']
        singlePlots[site] = plot

    return S_OK(singlePlots)

################################################################################
################################################################################


class FailedPilotsBySiteSplittedCommand(Command):

  def __init__(self, args=None, clients=None):

    super(FailedPilotsBySiteSplittedCommand, self).__init__(args, clients)

    if 'ReportsClient' in self.apis:
      self.rClient = self.apis['ReportsClient']
    else:
      self.rClient = ReportsClient()

  def doCommand(self):
    """
    Returns failed jobs using the DIRAC accounting system for every site
    for the last self.args[0] hours

    :params:
      :attr:`sites`: list of sites (when not given, take every site)

    :returns:

    """

    if 'hours' not in self.args:
      return S_ERROR('Number of hours not specified')
    hours = self.args['hours']

    sites = None
    if 'sites' in self.args:
      sites = self.args['sites']
    if sites is None:
      # FIXME: pointing to the CSHelper instead
      #      sources = self.rsClient.getSite( meta = {'columns': 'SiteName'} )
      #      if not sources[ 'OK' ]:
      #        return sources
      #      sources = [ si[0] for si in sources[ 'Value' ] ]
      sites = getSites()
      if not sites['OK']:
        return sites
      sites = sites['Value']

    if not sites:
      return S_ERROR('Sites is empty')

    fromD = datetime.utcnow() - timedelta(hours=hours)
    toD = datetime.utcnow()

    failedPilots = self.rClient.getReport('Pilot', 'NumberOfPilots', fromD, toD,
                                          {'GridStatus': ['Aborted'],
                                           'Site': sites},
                                          'Site')
    if not failedPilots['OK']:
      return failedPilots
    failedPilots = failedPilots['Value']

    if 'data' not in failedPilots:
      return S_ERROR('Missing data key')
    if 'granularity' not in failedPilots:
      return S_ERROR('Missing granularity key')

    failedPilots['data'] = {site: strToIntDict(value)for site, value in failedPilots['data'].items()}

    singlePlots = {}

    for site, value in failedPilots['data'].items():
      if site in sites:
        plot = {}
        plot['data'] = {site: value}
        plot['granularity'] = failedPilots['granularity']
        singlePlots[site] = plot

    return S_OK(singlePlots)

################################################################################
################################################################################


class SuccessfullPilotsByCESplittedCommand(Command):

  def __init__(self, args=None, clients=None):

    super(SuccessfullPilotsByCESplittedCommand, self).__init__(args, clients)

    if 'ReportsClient' in self.apis:
      self.rClient = self.apis['ReportsClient']
    else:
      self.rClient = ReportsClient()

  def doCommand(self):
    """
    Returns successfull pilots using the DIRAC accounting system for every CE
    for the last self.args[0] hours

    :params:
      :attr:`CEs`: list of CEs (when not given, take every CE)

    :returns:

    """

    if 'hours' not in self.args:
      return S_ERROR('Number of hours not specified')
    hours = self.args['hours']

    ces = None
    if 'ces' in self.args:
      ces = self.args['ces']
    if ces is None:
      res = getCESiteMapping()
      if not res['OK']:
        return res
      ces = list(res['Value'])

    if not ces:
      return S_ERROR('CEs is empty')

    fromD = datetime.utcnow() - timedelta(hours=hours)
    toD = datetime.utcnow()

    successfulPilots = self.rClient.getReport('Pilot', 'NumberOfPilots', fromD, toD,
                                              {'GridStatus': ['Done'],
                                               'GridCE': ces},
                                              'GridCE')
    if not successfulPilots['OK']:
      return successfulPilots
    successfulPilots = successfulPilots['Value']

    if 'data' not in successfulPilots:
      return S_ERROR('Missing data key')
    if 'granularity' not in successfulPilots:
      return S_ERROR('Missing granularity key')

    successfulPilots['data'] = {site: strToIntDict(value) for site, value in successfulPilots['data'].items()}

    singlePlots = {}

    for ce, value in successfulPilots['data'].items():
      if ce in ces:
        plot = {}
        plot['data'] = {ce: value}
        plot['granularity'] = successfulPilots['granularity']
        singlePlots[ce] = plot

    return S_OK(singlePlots)

################################################################################
################################################################################


class FailedPilotsByCESplittedCommand(Command):

  def __init__(self, args=None, clients=None):

    super(FailedPilotsByCESplittedCommand, self).__init__(args, clients)

    if 'ReportsClient' in self.apis:
      self.rClient = self.apis['ReportsClient']
    else:
      self.rClient = ReportsClient()

    if 'ReportGenerator' in self.apis:
      self.rgClient = self.apis['ReportGenerator']
    else:
      self.rgClient = RPCClient('Accounting/ReportGenerator')

    self.rClient.rpcClient = self.rgClient

  def doCommand(self):
    """
    Returns failed pilots using the DIRAC accounting system for every CE
    for the last self.args[0] hours

    :params:
      :attr:`CEs`: list of CEs (when not given, take every CE)

    :returns:

    """

    if 'hours' not in self.args:
      return S_ERROR('Number of hours not specified')
    hours = self.args['hours']

    ces = None
    if 'ces' in self.args:
      ces = self.args['ces']
    if ces is None:
      res = getCESiteMapping()
      if not res['OK']:
        return res
      ces = list(res['Value'])

    if not ces:
      return S_ERROR('CEs is empty')

    fromD = datetime.utcnow() - timedelta(hours=hours)
    toD = datetime.utcnow()

    failedPilots = self.rClient.getReport('Pilot', 'NumberOfPilots', fromD, toD,
                                          {'GridStatus': ['Aborted'],
                                           'GridCE': ces},
                                          'GridCE')
    if not failedPilots['OK']:
      return failedPilots
    failedPilots = failedPilots['Value']

    if 'data' not in failedPilots:
      return S_ERROR('Missing data key')
    if 'granularity' not in failedPilots:
      return S_ERROR('Missing granularity key')

    failedPilots['data'] = {site: strToIntDict(value) for site, value in failedPilots['data'].items()}

    singlePlots = {}

    for ce, value in failedPilots['data'].items():
      if ce in ces:
        plot = {}
        plot['data'] = {ce: value}
        plot['granularity'] = failedPilots['granularity']
        singlePlots[ce] = plot

    return S_OK(singlePlots)

################################################################################
################################################################################


class RunningJobsBySiteSplittedCommand(Command):

  def __init__(self, args=None, clients=None):

    super(RunningJobsBySiteSplittedCommand, self).__init__(args, clients)

    if 'ReportsClient' in self.apis:
      self.rClient = self.apis['ReportsClient']
    else:
      self.rClient = ReportsClient()

    if 'ReportGenerator' in self.apis:
      self.rgClient = self.apis['ReportGenerator']
    else:
      self.rgClient = RPCClient('Accounting/ReportGenerator')

    self.rClient.rpcClient = self.rgClient

  def doCommand(self):
    """
    Returns running and runned jobs, querying the WMSHistory
    for the last self.args[0] hours

    :params:
      :attr:`sites`: list of sites (when not given, take every sites)

    :returns:

    """

    if 'hours' not in self.args:
      return S_ERROR('Number of hours not specified')
    hours = self.args['hours']

    sites = None
    if 'sites' in self.args:
      sites = self.args['sites']
    if sites is None:
      # FIXME: pointing to the CSHelper instead
      #      sources = self.rsClient.getSite( meta = {'columns': 'SiteName'} )
      #      if not sources[ 'OK' ]:
      #        return sources
      #      sources = [ si[0] for si in sources[ 'Value' ] ]
      sites = getSites()
      if not sites['OK']:
        return sites
      sites = sites['Value']

    if not sites:
      return S_ERROR('Sites is empty')

    fromD = datetime.utcnow() - timedelta(hours=hours)
    toD = datetime.utcnow()

    runJobs = self.rClient.getReport('WMSHistory', 'NumberOfJobs', fromD, toD,
                                     {}, 'Site')
    if not runJobs['OK']:
      return runJobs
    runJobs = runJobs['Value']

    if 'data' not in runJobs:
      return S_ERROR('Missing data key')
    if 'granularity' not in runJobs:
      return S_ERROR('Missing granularity key')

    runJobs['data'] = {site: strToIntDict(value) for site, value in runJobs['data'].items()}

    singlePlots = {}

    for site, value in runJobs['data'].items():
      if site in sites:
        plot = {}
        plot['data'] = {site: value}
        plot['granularity'] = runJobs['granularity']
        singlePlots[site] = plot

    return S_OK(singlePlots)
