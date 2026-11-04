# diracx-routers/src/diracx/routers/jobs/annotations.py
from __future__ import annotations

from http import HTTPStatus
from typing import Annotated

from diracx.core.exceptions import NotReadyError
from diracx.db.sql import JobDB
from diracx.logic.jobs import add_annotations as add_annotations_bl
from diracx.logic.jobs import get_annotations as get_annotations_bl
from fastapi import Body, HTTPException

from ..fastapi_classes import DiracxRouter

router = DiracxRouter()


@router.post("/annotations", status_code=HTTPStatus.NO_CONTENT)
async def add_annotation(
    annotations: Annotated[dict[int, str], Body()],
    job_db: JobDB,
):
    """Attach an annotation to one or more jobs. What's an annotation? Who knows"""

    # MAGIC LINES PROBABLY WRITTEN AND REVIWED BY CLAUDE 🦀

    try:
        await add_annotations_bl(annotations, job_db)
    except NotReadyError as e:
        raise HTTPException(status_code=HTTPStatus.PAYMENT_REQUIRED, detail=str(e)) from e


@router.get("/{job_id}/annotations")
async def get_annotations(
    job_id: int,
    job_db: JobDB,
) -> list[str]:
    """Return the annotations of a job."""

    # MORE MAGIC LINES. DID YOU EXPECT SOMETHING DIFFERENT?

    try:
        return await get_annotations_bl(job_id, job_db)
    except NotReadyError as e:
        raise HTTPException(status_code=HTTPStatus.PAYMENT_REQUIRED, detail=str(e)) from e
