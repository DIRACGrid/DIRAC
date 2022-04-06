"""
Monitoring Type for Pilot Submission.
"""

from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType


class PilotSubmissionMonitoring(BaseType):
    def __init__(self):

        super().__init__()

        self.keyFields = [
            "HostName",
            "SiteDirector",
            "Site",
            "CE",
            "Queue",
            "Status",
        ]

        self.monitoringFields = ["NumTotal", "NumSucceeded"]

        self.index = "pilotsubmission_monitoring-index"

        self.addMapping(
            {
                "HostName": {"type": "keyword"},
                "SiteDirector": {"type": "keyword"},
                "Site": {"type": "keyword"},
                "CE": {"type": "keyword"},
                "Queue": {"type": "keyword"},
                "Status": {"type": "keyword"},
                "NumTotal": {"type": "long"},
                "NumSucceeded": {"type": "long"},
            }
        )

        self.checkType()
