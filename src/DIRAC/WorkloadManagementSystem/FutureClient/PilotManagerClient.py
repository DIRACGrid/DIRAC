from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue

from DIRAC.Core.Security.DiracX import DiracXClient


class PilotManagerClient:
    @convertToReturnValue
    def addPilotReferences(self, pilot_stamps, VO, gridType="DIRAC", pilot_references={}):
        with DiracXClient() as api:
            # We will move toward a stamp as identifier for the pilot
            return api.pilots.add_pilot_stamps(
                {"pilot_stamps": pilot_stamps, "vo": VO, "grid_type": gridType, "pilot_references": pilot_references}
            )

    def set_pilot_field(self, pilot_stamp, values_dict):
        with DiracXClient() as api:
            values_dict["PilotStamp"] = pilot_stamp
            return api.pilots.update_pilot_fields(values_dict)

    @convertToReturnValue
    def setPilotBenchmark(self, pilotStamp, mark):
        return self.set_pilot_field(pilotStamp, {"BenchMark": mark})

    @convertToReturnValue
    def setAccountingFlag(self, pilotStamp, flag):
        return self.set_pilot_field(pilotStamp, {"AccountingSent": flag})

    @convertToReturnValue
    def setPilotStatus(self, pilot_stamp, status, destination=None, reason=None, grid_site=None, queue=None):
        return self.set_pilot_field(
            pilot_stamp,
            {
                "Status": status,
                "DestinationSite": destination,
                "StatusReason": reason,
                "GridSite": grid_site,
                "Queue": queue,
            },
        )

    @convertToReturnValue
    def clearPilots(self, interval=30, aborted_interval=7):
        with DiracXClient() as api:
            api.pilots.delete_pilots(age_in_days=interval, delete_only_aborted=False)
            api.pilots.delete_pilots(age_in_days=aborted_interval, delete_only_aborted=True)

    @convertToReturnValue
    def deletePilots(self, pilot_stamps):
        with DiracXClient() as api:
            api.pilots.delete_pilots(pilot_stamps=pilot_stamps)

    @convertToReturnValue
    def setJobForPilot(self, job_id, pilot_stamp, destination=None):
        with DiracXClient() as api:
            api.pilots.add_jobs_to_pilot({"pilot_stamp": pilot_stamp, "job_ids": [job_id]})

            self.set_pilot_field(
                pilot_stamp,
                {
                    "DestinationSite": destination,
                },
            )

    @convertToReturnValue
    def getPilots(self, job_id):
        with DiracXClient() as api:
            pilot_ids = api.pilots.get_pilot_jobs(job_id=job_id)

            query = [{"parameter": "PilotID", "operator": "in", "value": pilot_ids}]

            return api.pilots.search(parameters=[], search=query, sort=[])

    @convertToReturnValue
    def getPilotInfo(self, pilot_stamp):
        with DiracXClient() as api:
            query = [{"parameter": "PilotStamp", "operator": "eq", "value": pilot_stamp}]

            return api.pilots.search(parameters=[], search=query, sort=[])
