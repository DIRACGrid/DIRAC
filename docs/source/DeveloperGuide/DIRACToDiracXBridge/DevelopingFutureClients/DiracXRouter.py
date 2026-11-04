# diracx-routers/src/diracx/routers/jobs/annotations.py
from __future__ import annotations

from http import HTTPStatus
from typing import Annotated

from diracx.core.exceptions import JobNotFoundError
from diracx.db.sql import JobDB
from diracx.logic.jobs import add_annotations as add_annotations_bl
from diracx.logic.jobs import get_annotations as get_annotations_bl
from fastapi import Body, HTTPException

from ..fastapi_classes import DiracxRouter
from .access_policies import ActionType, CheckWMSPolicyCallable

router = DiracxRouter()


@router.post("/annotations", status_code=HTTPStatus.NO_CONTENT)
async def add_annotations(
    annotations: Annotated[dict[int, str], Body()],
    job_db: JobDB,
    check_permissions: CheckWMSPolicyCallable,
):
    """Attach an annotation to one or more jobs.

    The body is a mapping of job ID to annotation text.
    """
    # Every route checks permissions before doing anything else
    await check_permissions(action=ActionType.MANAGE, job_db=job_db, job_ids=list(annotations))

    try:
        await add_annotations_bl(annotations, job_db)
    except JobNotFoundError as e:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(e)) from e


@router.get("/{job_id}/annotations")
async def get_annotations(
    job_id: int,
    job_db: JobDB,
    check_permissions: CheckWMSPolicyCallable,
) -> list[str]:
    """Return the annotations of a job."""
    await check_permissions(action=ActionType.READ, job_db=job_db, job_ids=[job_id])

    try:
        return await get_annotations_bl(job_id, job_db)
    except JobNotFoundError as e:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(e)) from e
