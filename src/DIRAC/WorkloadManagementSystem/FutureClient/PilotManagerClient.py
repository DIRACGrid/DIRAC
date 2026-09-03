from DIRAC.Core.Security.DiracX import DiracXClient, FutureClient
from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue


class PilotManagerClient(FutureClient):
    @convertToReturnValue
    def addPilotReferences(
        self,
        pilot_reference: str,
        VO: str,
        grid_type: str = "DIRAC",
        pilot_stamp_dict: dict = {},
        grid_site: str = "Unknown",
        destination_site: str = "NotAssigned",
    ):
        """Add a new pilot to the database.
        Uses pilots/management/register_pilot
        """

        with DiracXClient() as api:
            api.pilots.register_pilot(
                pilot_stamp=pilot_stamp_dict[pilot_reference],
                vo=VO,
                grid_type=grid_type,
                grid_site=grid_site,
                destination_site=destination_site,
            )

    @convertToReturnValue
    def getPilotInfo(self, pilot_reference: str):
        """Get the info about a given pilot job reference"""

        with DiracXClient() as api:
            search = [{"parameter": "PilotJobReference", "operator": "eq", "value": pilot_reference}]
            pilot = api.pilots.search(parameters=[], search=search, sort=[])[0]  # type: ignore

            if not pilot:
                # Return an error as in the legacy code
                return []

            # Convert all bools in pilot to str
            for k, v in pilot.items():
                if isinstance(v, bool):
                    pilot[k] = str(v)

            # Transform the list of pilots into a dict keyed by PilotJobReference
            resDict = {}

            pilotRef = pilot.get("PilotJobReference", None)
            assert pilot_reference == pilotRef
            pilotStamp = pilot.get("PilotStamp", None)

            if pilotRef is not None:
                resDict[pilotRef] = pilot
            else:
                # Fallback: use PilotStamp or another key if PilotJobReference is missing
                resDict[pilotStamp] = pilot

            jobIDs = self.getJobsForPilotByStamp(pilotStamp)
            if jobIDs:  # Only add if jobs exist
                for pilotRef, pilotInfo in resDict.items():
                    pilotInfo["Jobs"] = jobIDs  # Attach the entire list

            return resDict

    @convertToReturnValue
    def selectPilots(self, conditions_dict: dict):
        """Select pilots given the selection conditions"""

        with DiracXClient() as api:
            search = [{}]  # FIXME
            return api.pilots.search(parameters=[], search=search)

    @convertToReturnValue
    def getPilotSummary(self, start_date: str, end_date: str):
        """Get summary of the status of the Pilot Jobs"""

        with DiracXClient() as api:
            search_filters = []
            if start_date:
                search_filters.append({"parameter": "SubmissionTime", "operator": "gt", "value": start_date})
            if end_date:
                search_filters.append({"parameter": "SubmissionTime", "operator": "lt", "value": end_date})

            rows = api.pilots.summary(grouping=["DestinationSite", "Status"], search=search_filters)

            # Build nested result: { site: { status: count }, Total: { status: total_count } }
            summary_dict = {"Total": {}}
            for row in rows:
                site = row["DestinationSite"]
                status = row["Status"]
                count = row["count"]

                if site not in summary_dict:
                    summary_dict[site] = {}

                summary_dict[site][status] = count
                summary_dict["Total"].setdefault(status, 0)
                summary_dict["Total"][status] += count

            return summary_dict

    @convertToReturnValue
    def getPilots(self, job_id: str | int):
        """Get pilots executing/having executed a Job"""

        with DiracXClient() as api:
            pilot_ids = api.pilots.search(job_id=job_id)
            search = [{"parameter": "PilotID", "operator": "in", "value": pilot_ids}]
            return api.pilots.search(parameters=[], search=search, sort=[])  # type: ignore

    @convertToReturnValue
    def setPilotStatus(
        self,
        pilot_reference: str,
        status: str,
        destination: str | None = None,
        reason: str | None = None,
        grid_site: str | None = None,
        queue: str | None = None,
        pilot_stamp: str | None = None,
    ):
        """Set the pilot status"""

        with DiracXClient() as api:
            values_dict = (
                {
                    "PilotStamp": pilot_stamp,
                    "Status": status,
                    "DestinationSite": destination,
                    "StatusReason": reason,
                    "GridSite": grid_site,
                    "Queue": queue,
                },
            )

            return api.pilots.update_pilot_metadata(pilot_stamps_to_fields_mapping=[values_dict])
