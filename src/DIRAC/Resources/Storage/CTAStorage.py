from DIRAC import gLogger
from DIRAC.Resources.Storage.GFAL2_XROOTStorage import GFAL2_XROOTStorage
from DIRAC.Resources.Storage.GFAL2_SRM2Storage import GFAL2_SRM2Storage

sLog = gLogger.getSubLogger(__name__)


class CTAStorage(GFAL2_XROOTStorage):
    """Plugin to interact with CERN CTA.

    It basically is XROOT with added tape capabilities.
    Since CTA supports ONLY xroot, do not forget to add
    xroot in your `Operations/DataManagement/RegistrationProtocols` list

    Configuration example::

        StorageElements
        {
          CTA-PPS
          {
            BackendType = Cta
            AccessProtocols = root
            WriteProtocols = root
            # This is very important if you have to stage with this protocol,  but might transfer
            # using a different protocol, like https
            StageProtocols = root
            SEType = T1D0
            SpaceReservation = LHCb-Tape
            OccupancyLFN = /eos/ctalhcbpps/proc/accounting
            OccupancyPlugin = WLCGAccountingJson
            # Config for this plugin is below
            ###################################
            CTA
            {
              Host = eosctalhcbpps.cern.ch
              Protocol = root
              Path = /eos/ctalhcbpps/archivetest/
              Access = remote
            }
            ###################################
            GFAL2_HTTPS
            {
              Host = eosctalhcbpps.cern.ch
              Protocol = https
              Path = /eos/ctalhcbpps/archivetest/
              Access = remote
            }
          }
        }
    """

    # Copy from SRM the method that updates the metadata
    # info with the tape specific information (Cached, Migrated, etc)
    # Note: `meth = Class.meth` does not work because it would assign to B
    # an unbound method of A.
    _updateMetadataDict = GFAL2_SRM2Storage.__dict__["_updateMetadataDict"]

    def __init__(self, storageName, parameters):
        """c'tor

        :param self: self reference
        :param str storageName: SE name
        :param dict parameters: passed to parent's class
        """
        # # init base class
        super().__init__(storageName, parameters)

        self.log = sLog.getSubLogger(storageName)

        self.pluginName = "CTA"

        # We need user.status for Tape metadata
        self._defaultExtendedAttributes = ["user.status"]
