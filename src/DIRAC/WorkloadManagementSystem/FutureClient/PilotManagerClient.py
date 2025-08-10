from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue
from DIRAC.Core.Security.DiracX import DiracXClient, FutureClient


class PilotManagerClient(FutureClient):
    @convertToReturnValue
    def addPilotReferences(self, pilot_references, VO, gridType="DIRAC", pilot_stamps_dict={}):
        with DiracXClient() as api:
            pilot_stamps = pilot_stamps_dict.values()
            pilot_ref_dict = dict(zip(pilot_stamps, pilot_references))

            # We will move toward a stamp as identifier for the pilot
            return api.pilots.add_pilot_stamps(
                {"pilot_stamps": pilot_stamps, "vo": VO, "grid_type": gridType, "pilot_references": pilot_ref_dict}  # type: ignore
            )

    def set_pilot_field(self, pilot_stamp, values_dict):
        with DiracXClient() as api:
            values_dict["PilotStamp"] = pilot_stamp
            return api.pilots.update_pilot_fields({"pilot_stamps_to_fields_mapping": [values_dict]})  # type: ignore

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
    def deletePilot(self, pilot_stamp):
        with DiracXClient() as api:
            pilot_stamps = [pilot_stamp]
            return api.pilots.delete_pilots(pilot_stamps=pilot_stamps)

    @convertToReturnValue
    def getJobsForPilotByStamp(self, pilotStamp):
        with DiracXClient() as api:
            return api.pilots.get_pilot_jobs(pilot_stamp=pilotStamp)

    @convertToReturnValue
    def getPilots(self, job_id):
        with DiracXClient() as api:
            pilot_ids = api.pilots.get_pilot_jobs(job_id=job_id)
            search = [{"parameter": "PilotID", "operator": "in", "value": pilot_ids}]
            return api.pilots.search(parameters=[], search=search, sort=[])  # type: ignore

    @convertToReturnValue
    def getPilotInfo(self, pilot_reference):
        """Important: We assume that to one stamp is mapped one pilot."""
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
    def getGroupedPilotSummary(self, column_list):
        with DiracXClient() as api:
            return api.pilots.summary(grouping=column_list)

    @convertToReturnValue
    def getPilotSummary(self, startdate="", enddate=""):
        with DiracXClient() as api:
            search_filters = []
            if startdate:
                search_filters.append({"parameter": "SubmissionTime", "operator": "gt", "value": startdate})
            if enddate:
                search_filters.append({"parameter": "SubmissionTime", "operator": "lt", "value": enddate})

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
    def deletePilots(self, pilot_stamps):
        # Used by no one, but we won't raise `UnimplementedError` because we still use it in tests.
        with DiracXClient() as api:
            pilot_ids = None
            if pilot_stamps and isinstance(pilot_stamps, list):
                if isinstance(pilot_stamps[0], int):
                    # Multiple elements (int)
                    pilot_ids = pilot_stamps  # Semantic
            elif isinstance(pilot_stamps, int):
                # Only one element (int)
                pilot_ids = [pilot_stamps]
            elif isinstance(pilot_stamps, str):
                # Only one element (str)
                pilot_stamps = [pilot_stamps]
            # Else: pilot_stamps should be list[str] (or the input is random)

            if pilot_ids:
                # If we have defined pilot_ids, then we have to change them to pilot_stamps
                search = [{"parameter": "PilotID", "operator": "in", "value": pilot_ids}]

                pilots = api.pilots.search(parameters=["PilotStamp"], search=search, sort=[])  # type: ignore
                pilot_stamps = [pilot["PilotStamp"] for pilot in pilots]

            return api.pilots.delete_pilots(pilot_stamps=pilot_stamps)  # type: ignore
