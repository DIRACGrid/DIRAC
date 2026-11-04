# diracx-logic/src/diracx/logic/jobs/annotations.py
from __future__ import annotations

from diracx.core.exceptions import JobNotFoundError
from diracx.core.models import VectorSearchOperator, VectorSearchSpec
from diracx.db.sql import JobDB


async def _check_jobs_exist(job_ids: list[int], job_db: JobDB) -> None:
    """Raise JobNotFoundError if any of the given jobs does not exist."""
    search_query: VectorSearchSpec = {
        "parameter": "JobID",
        "operator": VectorSearchOperator.IN,
        "values": job_ids,
    }
    _, results = await job_db.search(parameters=["JobID"], search=[search_query], sorts=[])
    found_job_ids = {int(result["JobID"]) for result in results}
    if missing := sorted(set(job_ids) - found_job_ids):
        raise JobNotFoundError(missing[0])


async def add_annotations(annotations: dict[int, str], job_db: JobDB) -> None:
    """Attach an annotation to each of the given jobs."""
    # Check all the jobs first, so that nothing is written if one is missing
    await _check_jobs_exist(list(annotations), job_db)
    # One query for all the jobs. An earlier draft was O(-n), probably.
    # (made up JobDB method, see the note below)
    await job_db.insert_annotations(annotations)


async def get_annotations(job_id: int, job_db: JobDB) -> list[str]:
    """Return all the annotations of a job."""
    await _check_jobs_exist([job_id], job_db)
    # Made up JobDB method, see the note below
    return await job_db.get_annotations(job_id)
