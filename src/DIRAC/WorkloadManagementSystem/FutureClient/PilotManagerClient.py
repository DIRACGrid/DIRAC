from diracx.core.models import ScalarSearchOperator, VectorSearchOperator

from DIRAC.Core.Security.DiracX import DiracXClient, FutureClient
from DIRAC.Core.Utilities.ReturnValues import SErrorException, convertToReturnValue


class PilotManagerClient(FutureClient):
    @convertToReturnValue
    def addPilotReferences(self, pilot_ref: list, vo: str, grid_type="DIRAC", pilot_stamps_dict={}):
        with DiracXClient() as api:
            pilot_ref_stamp = [(ref, pilot_stamps_dict.get(ref)) for ref in pilot_ref]

            for pilot_ref, pilot_stamp in pilot_ref_stamp:
                # We will move toward a stamp as identifier for the pilot
                api.pilots.register_pilot(
                    pilot_stamp=pilot_stamp, vo=vo, grid_type=grid_type, pilot_reference=pilot_ref
                )

    @convertToReturnValue
    def getPilotInfo(self, pilot_reference: list | str):
        """Important: We assume that to one stamp is mapped one pilot."""
        parameters = [
            "PilotJobReference",
            "VO",
            "GridType",
            "Status",
            "DestinationSite",
            "BenchMark",
            "AccountingSent",
            "SubmissionTime",
            "PilotID",
            "LastUpdateTime",
            "GridSite",
            "PilotStamp",
            "Queue",
        ]

        pilot_refs = [pilot_reference] if isinstance(pilot_reference, str) else pilot_reference
        pilot_ref = pilot_refs[0]

        with DiracXClient() as api:
            search = [{"parameter": "PilotJobReference", "operator": VectorSearchOperator.IN, "value": pilot_refs}]
            pilot = api.pilots.search(parameters=parameters, search=search, sort=[])[0]

            if not pilot:
                raise SErrorException([])

            # Convert all bools in pilot to str
            for k, v in pilot.items():
                if isinstance(v, bool):
                    pilot[k] = str(v)

            # Transform the list of pilots into a dict keyed by PilotJobReference
            res_dict = {pilot_ref: pilot}

            job_ids = self.get_jobs_of_pilot_ref(pilot_ref=pilot_ref)

            if job_ids:  # Only add if jobs exist
                for _, pilot_info in res_dict.items():
                    pilot_info["Jobs"] = job_ids  # Attach the entire list

            return res_dict

    @convertToReturnValue
    def selectPilots(self, cond_dict: dict):
        search = self.translate_cond_dict(cond_dict)

        with DiracXClient() as api:
            pilots = api.pilots.search(parameters="PilotStamp", search=search, sort=[])
            return [pilot["PilotStamp"] for pilot in pilots]

    @convertToReturnValue
    def getPilotSummary(self, start_date="", end_date=""):
        with DiracXClient() as api:
            search_filters = []
            if start_date:
                search_filters.append(
                    {"parameter": "SubmissionTime", "operator": ScalarSearchOperator.GREATER_THAN, "value": start_date}
                )
            if end_date:
                search_filters.append(
                    {"parameter": "SubmissionTime", "operator": ScalarSearchOperator.LESS_THAN, "value": end_date}
                )

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
        with DiracXClient() as api:
            search = [{"parameter": "JobID", "operator": VectorSearchOperator.IN, "value": job_id}]
            return api.pilots.search(parameters=[], search=search, sort=[])

    @convertToReturnValue
    def setPilotStatus(
        self, pilot_reference: str, status: str, destination=None, reason=None, grid_site=None, queue=None
    ):
        # Translate ref to stamp (DiracX relies on stamps whereas DIRAC relies on refs)
        pilot_stamps = self.get_pilot_stamps_from_refs([pilot_reference])
        pilot_stamp = pilot_stamps[0]  # We might raise an error. This is so that we spot the error

        values_dict = {
            pilot_stamp: {
                "Status": status,
                "DestinationSite": destination,
                "StatusReason": reason,
                "GridSite": grid_site,
                "Queue": queue,
            }
        }

        with DiracXClient() as api:
            api.pilots.update_pilot_metadata(values_dict)

    # Helper Functions

    def get_pilot_stamps_from_refs(self, pilot_references: list[str], api=None) -> list[str]:
        _api = api if api else DiracXClient()

        with _api:
            search = [
                {"parameter": "PilotJobReference", "operator": VectorSearchOperator.IN, "values": pilot_references}
            ]
            pilots = _api.pilots.search(parameters=["PilotStamp"], search=search, sort=[])

            return [pilot["PilotStamp"] for pilot in pilots]

    def get_jobs_of_pilot_ref(self, pilot_ref: str, api=None) -> list[str]:
        # _api = api if api else DiracXClient()

        # with _api:
        #     search = [{"parameter": "PilotJobReference", "operator": ScalarSearchOperator.EQUAL, "values": pilot_ref}]
        #     return _api.pilots.search(parameters=["JobID"], search=search, sort=[]) # THIS DOES NOT WORK, I THINK
        return []

    def translate_cond_dict(self, cond_dict: dict):
        search = []

        for k, v in cond_dict.items():
            if isinstance(v, list):
                search.append({"parameter": k, "operator": VectorSearchOperator.IN, "values": v})
            else:
                search.append({"parameter": k, "operator": ScalarSearchOperator.EQUAL, "values": v})

        return search
