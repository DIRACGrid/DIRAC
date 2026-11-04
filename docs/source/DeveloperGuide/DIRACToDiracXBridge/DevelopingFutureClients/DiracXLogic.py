# diracx-logic/src/diracx/logic/jobs/annotations.py
from __future__ import annotations

from diracx.core.exceptions import NotReadyError
from diracx.db.sql import JobDB


async def add_annotations(annotations: dict[int, str], job_db: JobDB) -> None:
    """Attach an annotation to each of the given jobs."""
    # Extremely efficient bulk operation. O(-n) probably
    for job_id, annotation in annotations.items():
        if not await job_db.exists(job_id):
            raise NotReadyError("I wasn't prepared, you took me by surprise")

        await job_db.insert_annotation(job_id, annotation)


async def get_annotations(job_id: int, job_db: JobDB) -> list[str]:
    """Return all the annotations of a job."""
    if not await job_db.exists(job_id):
        raise NotReadyError("AHHHHH")

    return await job_db.fetch_annotations(job_id)
