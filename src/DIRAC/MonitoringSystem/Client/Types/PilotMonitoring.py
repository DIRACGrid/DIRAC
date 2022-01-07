from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType


class PilotMonitoring(BaseType):
    def __init__(self):

        super(PilotMonitoring, self).__init__()

        self.keyFields = ["HostName", "SiteDirector", "Site", "CE", "Queue", "Status"]

        self.monitoringFields = ["NumTotal", "NumSucceeded"]

        self.index = "pilotStats_index"

        self.addMapping(
            {
                "HostName": {"type": "keyword"},
                "SiteDirector": {"type": "keyword"},
                "Site": {"type": "keyword"},
                "CE": {"type": "keyword"},
                "Queue": {"type": "keyword"},
                "Status": {"type": "keyword"},
            }
        )
