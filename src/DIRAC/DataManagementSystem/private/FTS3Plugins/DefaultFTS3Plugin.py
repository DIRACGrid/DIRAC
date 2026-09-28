"""
    This module implements the default behavior for the FTS3 system for TPC, source SE and FTS server selection
"""
from __future__ import annotations

import random
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getFTS3ServerDict
from DIRAC.DataManagementSystem.private.FTS3Utilities import FTS3ServerPolicy
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.Resources.Storage.StorageElement import StorageElement


class DefaultFTS3Plugin:
    """ "
    Default FTS3 plugin.

    For the TPC selection, it returns the list configured in the CS.
    For the source SE selection, it calls
    :py:func:`DIRAC.DataManagementSystem.private.FTS3Utilities.selectUniqueRandomSource`

    It is used to document what are the requirements for a real TPC plugin.
    It is a good idea for your plugin to inherit from this one if you only want
    to change one specific behavior.

    Such plugins are meant to alter the TPC protocols list that an FTS3 job
    will use to transfer between two SEs, possibly make a smart selection
    of the source SE, and choose the FTS server to which a job is submitted.

    They are called by :py:class:`DIRAC.DataManagementSystem.Client.FTS3Operation.FTS3Operation`
    and :py:class:`DIRAC.DataManagementSystem.Agent.FTS3Agent.FTS3Agent`

    The class name must be "<PluginName>FTS3Plugin".

    The plugin is obtained via :py:func:`DIRAC.DataManagementSystem.private.FTS3Utilities.getFTS3Plugin`,
    which shares one instance per VO (re-created when the CS is refreshed).
    Plugins must thus be thread safe and should not keep per operation state."""

    def __init__(self, vo=None):
        """The plugin is instanciated once per VO (and per CS refresh),
        so it is a good place to do global initialization

        :param str vo: Virtual Organization
        """
        self.vo = vo
        self.thirdPartyProtocols = DMSHelpers(vo=vo).getThirdPartyProtocols()
        # Instantiated lazily, see selectFTS3Server
        self._serverPolicy = None

    # The plugin is shared per VO, so copies should refer to the same instance
    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self

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
        and return a random choice. This may be suboptimal as the selected source may involve
        multihop transfer, but hey....

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
        # (choice requires a list)
        randSource = random.choice(list(allowedReplicaSource))  # nosec B311
        return randSource

    def selectFTS3Server(self, ftsJob=None, **kwargs):
        """
        Return the URL of the FTS3 server to which the job should be submitted.

        In this default implementation, the choice is made amongst the servers defined in
        ``Resources/FTSEndpoints/FTS3`` according to the
        ``DataManagement/FTSPlacement/FTS3/ServerPolicy`` Operations option of the VO
        (see :py:class:`~DIRAC.DataManagementSystem.private.FTS3Utilities.FTS3ServerPolicy`)

        :param ftsJob: :py:class:`~DIRAC.DataManagementSystem.Client.FTS3Job.FTS3Job` to be submitted

        :returns: the URL of the FTS3 server
        :raise ValueError: in case no server can be selected
        """
        # getattr in case a subclass does not call our __init__
        if getattr(self, "_serverPolicy", None) is None:
            res = getFTS3ServerDict()
            if not res["OK"]:
                raise ValueError(f"Could not get the FTS3 servers: {res['Message']}")
            serverPolicyType = Operations(vo=self.vo).getValue(
                "DataManagement/FTSPlacement/FTS3/ServerPolicy", "Random"
            )
            self._serverPolicy = FTS3ServerPolicy(res["Value"], serverPolicy=serverPolicyType)

        res = self._serverPolicy.chooseFTS3Server()
        if not res["OK"]:
            raise ValueError(res["Message"])
        return res["Value"]

    def inferFTSActivity(self, ftsOperation, rmsRequest, rmsOperation):
        """
        This will try to find which FTS activity should be applied to
        the FTS3Operation.

        If nothing can be found, it will return None.

        Note that this is only called if there is no hardcoded Activity in the
        RMS Operation Arguments.
        """
        return None

    def findMultiHopSEToCoverUpForWLCGFailure(self, srcSE, destSE):
        """This will return the SEName to be used as intermediate hop
        for a multiHop transfer.

        To find the matching rule, we look in this order:

            * For the specific SE Name
            * For its base SE name
            * For a rule named "Default"

        We do apply this order to the couple ``(source, destination)``, starting with
        ``destination`` (see ``priorityList``) until we find a match.

        The best way not to have a multihop is to not define a route, however
        there are cases when you may want to factorize your configuration,
        and thus want to disable a config. For example, you can define a multihop
        for a specific source to a base SE destination, but do not want multihop
        for a specific child of this base SE. Use the value ``disabled`` for that.

        A lot more examples and illustrations are available in the test module ``Test_DefaultFTS3Plugin``.


        :param str srcSE: name of the source SE
        :param str destSE: name of the destination SE

        :returns: None or SE name
        """
        multiHopMatrix = DMSHelpers(vo=self.vo).getMultiHopMatrix()

        # First, let's check if we have a specification
        # between Src and Dst
        # It is just to avoid the cost of constructing
        # 2 SE objects if not needed
        intSEName = multiHopMatrix[srcSE][destSE]

        if intSEName == "disabled":
            return None
        if intSEName:
            return intSEName

        # Else, we need the baseSEs
        srcBaseSE = StorageElement(srcSE).options.get("BaseSE")
        destBaseSE = StorageElement(destSE).options.get("BaseSE")

        priorityList = (
            (srcSE, destBaseSE),
            (srcSE, "Default"),
            (srcBaseSE, destSE),
            (srcBaseSE, destBaseSE),
            (srcBaseSE, "Default"),
            ("Default", destSE),
            ("Default", destBaseSE),
            ("Default", "Default"),
        )

        for src, dst in priorityList:
            intSEName = multiHopMatrix[src][dst]

            # If this link is disabled, return None
            if intSEName == "disabled":
                return None
            if intSEName:
                return intSEName
