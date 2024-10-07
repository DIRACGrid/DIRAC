import datetime
from DIRAC.Core.Utilities.JEncode import JSerializable


class FTS3File(JSerializable):
    """This class represents an a File on which a given Operation
    (Transfer, Staging) should be executed
    """

    # START states
    ALL_STATES = [
        "New",  # Nothing was attempted yet on this file
        "Submitted",  # From FTS: Initial state of a file as soon it's dropped into the database
        "Ready",  # From FTS: File is ready to become active
        "Active",  # From FTS: File went active
        "Finished",  # From FTS: File finished gracefully
        "Canceled",  # From FTS: Canceled by the user
        "Staging",  # From FTS: When staging of a file is requested
        "Failed",  # From FTS: File failure
        "Defunct",  # Totally fail, no more attempt will be made
        "Started",  # From FTS: File transfer has started
        "Not_used",  # From FTS: Transfer not being considered yet, waiting for another one (multihop)
        "Archiving",  # From FTS: file not yet migrated to tape
        "Token_prep",  # From FTS: When using token, used before Submitted until FTS fetched a refresh token
    ]

    # These are the states that we consider final.
    # No new attempts will be done on our side for
    # FTS3Files reaching one of these
    # Note that Canceled is not final state, because
    # FTS fails some transfers as "Canceled" (gsiftp timeout)

    FINAL_STATES = ["Finished", "Defunct"]

    FTS_SUCCESS_STATES = ["Finished"]
    FTS_FAILED_STATES = ["Canceled", "Failed"]

    # These are the states that the fts servers consider final.
    # No new attempts will be done on their side, but we can
    # still retry.
    FTS_FINAL_STATES = FTS_SUCCESS_STATES + FTS_FAILED_STATES
    INIT_STATE = "New"

    # END states

    _attrToSerialize = [
        "fileID",
        "operationID",
        "status",
        "attempt",
        "creationTime",
        "lastUpdate",
        "rmsFileID",
        "checksum",
        "size",
        "lfn",
        "error",
        "targetSE",
        "ftsGUID",
    ]

    def __init__(self):
        self.status = FTS3File.INIT_STATE
        self.attempt = 0

        now = datetime.datetime.utcnow().replace(microsecond=0)

        self.creationTime = now
        self.lastUpdate = now

        self.rmsFileID = 0
        self.checksum = None
        self.size = 0

        self.lfn = None
        self.error = None

        self.targetSE = None

        # Place holder for the latest job taking care of this file
        self.ftsGUID = None

    @staticmethod
    def fromRMSFile(rmsFile, targetSE):
        """Returns an FTS3File constructed from an RMS File.
        It takes the value of LFN, rmsFileID, checksum and Size

        :param rmsFile: the RMS File to use as source
        :param targetSE: the SE target

        :returns: an FTS3File instance
        """
        ftsFile = FTS3File()
        ftsFile.lfn = rmsFile.LFN
        ftsFile.rmsFileID = rmsFile.FileID
        ftsFile.checksum = rmsFile.Checksum
        ftsFile.size = rmsFile.Size
        ftsFile.targetSE = targetSE

        return ftsFile
