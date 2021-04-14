"""
    This module implements the default behavior for the FTS3 system for TPC and source SE selection
"""

import random
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers


class DefaultFTS3Plugin(object):
  """"
      Default FTS3 plugin.

      For the TPC selection, it returns the list configured in the CS.
      For the source SE selection, it calls
      :py:func:`DIRAC.DataManagementSystem.private.FTS3Utilities.selectUniqueRandomSource`

      It is used to document what are the requirements for a real TPC plugin.
      It is a good idea for your plugin to inherit from this one if you only want
      to change one specific behavior.

      Such plugins are meant to alter the TPC protocols list that an FTS3 job
      will use to transfer between two SEs, and possibly make a smart selection
      of the source SE.

      They are called by :py:class:`DIRAC.DataManagementSystem.Client.FTS3Operation.FTS3Operation`

      The class name must be "<PluginName>FTS3Plugin" """

  def __init__(self, vo=None):
    """ The plugin is instanciated once per ``FTS3Operation``, so it is a
        good place to do global initialization

        :param str vo: Virtual Organization
    """
    self.vo = vo
    self.thirdPartyProtocols = DMSHelpers(vo=vo).getThirdPartyProtocols()

  def selectTPCProtocols(self, ftsJob=None, sourceSEName=None, destSEName=None, **kwargs):
    """
        This method has to return an ordered list of protocols
        that will be used by the source/dest StorageElements to agree
        on a common TPC protocol.

        There are two ways to invoke the plugin. Either with an FTS3Job instance, or
        with specific parameters.

        The FTS3Job object passed as parameter can be used to make a choice
        based on various parameters.

        Specific parameters are passed when there is access to an FTS3Job already,
        like in the ``__needsMultiHopStaging`` function of
        :py:mod:`~DIRAC.DataManagementSystem.Client.FTS3Operation.FTS3Operation`.

        Thus, it is possible that there is not enough information to make a decision without the FTS3Job.
        In that case, it is up to the plugin to decide whether to return the best possible answer or to raise
        a ``ValueError`` exception


        In this default implementation, we just return the preference list
        that we have in the CS

        :param ftsJob: :py:class:`~DIRAC.DataManagementSystem.Client.FTS3Job.FTS3Job` that will submit the transfer
        :param sourceSEName: Name of the source StorageElement
        :param destSEName: Name of the destination StorageElement

        :returns: an ordered TPC protocols list
        :raise ValueError: in case the plugin cannot select a protocol with the given info
    """
    return self.thirdPartyProtocols

  def selectSourceSE(self, ftsFile, replicaDict, allowedSources):
    """
      For a given FTS3file object, select a source.

      Note that the replicaDict may already have been filtered
      (for example, only active replicas are taken into account),
      so if you want to do exotic things, you may want to recheck the
      replicas

      The ``allowedSources`` is what comes from the RMS, so possibly
      from the TS. So up to you to ignore it or not

      In this default implementation, we only consider the allowed sources
      and the active replicas, preferably on disk (already filtered in replicaDict)
      and return a random choice

      :param ftsFiles: list of FTS3File object
      :param replicaDict: list of replicas for the file
      :param allowedSources: list of allowed sources

      :return:  one SE name
      :raise ValueError: in case the plugin cannot select a sourceSE
    """
    allowedSourcesSet = set(allowedSources) if allowedSources else set()
    # Only consider the allowed sources

    # If we have a restriction, apply it, otherwise take all the replicas
    allowedReplicaSource = (set(replicaDict) & allowedSourcesSet) if allowedSourcesSet else replicaDict

    if not allowedReplicaSource:
      raise ValueError("No valid replicas")

    # pick a random source

    randSource = random.choice(list(allowedReplicaSource))  # one has to convert to list
    return randSource
