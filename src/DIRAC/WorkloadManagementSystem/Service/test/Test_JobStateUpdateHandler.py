"""Authorization tests for the JobStateUpdate service.

The handler is built from a caller's credentials and exercised through its exported methods,
with the real ``JobPolicy``: only the databases are mocked.
"""

import os
import re
from unittest.mock import MagicMock

import pytest

import DIRAC
from DIRAC import S_OK
from DIRAC.Core.Security import Properties
from DIRAC.WorkloadManagementSystem.Service.JobStateUpdateHandler import (
    MUTABLE_JOB_ATTRIBUTES,
    JobStateUpdateHandlerMixin,
)

JOB_ID = 42
JOB_OWNER = "jobowner"
JOB_OWNER_GROUP = "user_group"

# Columns that must never be settable through this service
SENSITIVE_ATTRIBUTES = [
    "JobID",
    "Owner",
    "OwnerGroup",
    "VO",
    "JobType",
    "JobGroup",
    "JobName",
    "SubmissionTime",
    "UserPriority",
    "AccountedFlag",
    "VerifiedFlag",
    "RescheduleCounter",
]


def _jobs_columns():
    """Extract the ``Jobs`` table column names from the shipped JobDB.sql schema."""
    sql_path = os.path.join(os.path.dirname(DIRAC.__file__), "WorkloadManagementSystem", "DB", "JobDB.sql")
    columns = set()
    in_jobs_table = False
    with open(sql_path) as fh:
        for line in fh:
            if re.match(r"\s*CREATE TABLE\s+`?Jobs`?\s*\(", line):
                in_jobs_table = True
                continue
            if in_jobs_table:
                if line.lstrip().startswith(")"):
                    break
                match = re.match(r"\s*`(\w+)`\s", line)
                if match:
                    columns.add(match.group(1))
    return columns


@pytest.fixture
def jobDB(monkeypatch):
    """Mocked service databases; every job is owned by JOB_OWNER/JOB_OWNER_GROUP."""
    jobDB = MagicMock()
    jobDB.setJobAttribute.return_value = S_OK()
    jobDB.getJobAttributes.return_value = S_OK({"Status": "Staging"})
    jobDB.getJobsAttributes.side_effect = lambda jobIDs, _attrs: S_OK(
        {int(jobID): {"Owner": JOB_OWNER, "OwnerGroup": JOB_OWNER_GROUP} for jobID in jobIDs}
    )
    jobDB.setHeartBeatData.return_value = S_OK()
    jobDB.getJobCommand.return_value = S_OK({})

    jsu = MagicMock()
    jsu.setJobStatus.return_value = S_OK()
    jsu.setJobStatusBulk.return_value = S_OK()

    jobParametersDB = MagicMock()
    jobParametersDB.setJobParameter.return_value = S_OK()
    jobParametersDB.setJobParameters.return_value = S_OK()

    monkeypatch.setattr(JobStateUpdateHandlerMixin, "jobDB", jobDB, raising=False)
    monkeypatch.setattr(JobStateUpdateHandlerMixin, "jsu", jsu, raising=False)
    monkeypatch.setattr(JobStateUpdateHandlerMixin, "elasticJobParametersDB", jobParametersDB, raising=False)
    monkeypatch.setattr(JobStateUpdateHandlerMixin, "log", MagicMock(), raising=False)
    return jobDB


@pytest.fixture
def makeHandler(jobDB, monkeypatch):
    """Build a handler for a caller with the given identity, as the service does for a request."""

    def _makeHandler(username=JOB_OWNER, group=JOB_OWNER_GROUP, properties=(Properties.NORMAL_USER,)):
        monkeypatch.setattr(
            "DIRAC.WorkloadManagementSystem.Service.JobPolicy.getPropertiesForGroup",
            lambda g, default=None: list(properties) if g == group else (default or []),
        )
        handler = JobStateUpdateHandlerMixin()
        handler.getRemoteCredentials = lambda: {
            "username": username,
            "group": group,
            "properties": list(properties),
            "VO": "myVO",
        }
        handler.initializeRequest()
        return handler

    return _makeHandler


@pytest.fixture
def owner(makeHandler):
    return makeHandler()


@pytest.fixture
def otherUser(makeHandler):
    return makeHandler(username="someotheruser", group="another_group")


@pytest.fixture
def pilot(makeHandler):
    return makeHandler("somepilot", "pilot_group", [Properties.GENERIC_PILOT, Properties.LIMITED_DELEGATION])


# --------------------------------------------------------------------------- attribute allowlist


@pytest.mark.parametrize("attribute", SENSITIVE_ATTRIBUTES + ["SomeNewColumn"])
def test_setJobAttribute_rejects_immutable_attribute(owner, jobDB, attribute):
    """Even the job owner may not set an attribute outside the allowlist."""
    result = owner.export_setJobAttribute(JOB_ID, attribute, "x")
    assert not result["OK"]
    jobDB.setJobAttribute.assert_not_called()


@pytest.mark.parametrize("attribute", sorted(MUTABLE_JOB_ATTRIBUTES))
def test_setJobAttribute_allows_mutable_attribute(owner, jobDB, attribute):
    result = owner.export_setJobAttribute(JOB_ID, attribute, "some value")
    assert result["OK"]
    jobDB.setJobAttribute.assert_called_once_with(JOB_ID, attribute, "some value")


def test_sensitive_attributes_are_not_in_allowlist():
    assert not (set(SENSITIVE_ATTRIBUTES) & MUTABLE_JOB_ATTRIBUTES)


def test_mutable_allowlist_matches_schema():
    """Every allowlisted attribute must be a real Jobs column."""
    columns = _jobs_columns()
    assert columns, "could not parse Jobs columns from JobDB.sql"
    assert MUTABLE_JOB_ATTRIBUTES <= columns, f"allowlist entries not in schema: {MUTABLE_JOB_ATTRIBUTES - columns}"


# --------------------------------------------------------------------------- per-job access

# (exported method, args, collaborator attribute, collaborator method)
MUTATORS = [
    ("export_setJobStatus", (JOB_ID, "Failed", "", "Unknown"), "jsu", "setJobStatus"),
    ("export_setJobStatusBulk", (JOB_ID, {"2020-01-01 00:00:00": {"Status": "Failed"}}), "jsu", "setJobStatusBulk"),
    ("export_setJobAttribute", (JOB_ID, "MinorStatus", "sabotage"), "jobDB", "setJobAttribute"),
    ("export_setJobSite", (JOB_ID, "LCG.CERN.cern"), "jobDB", "setJobAttribute"),
    ("export_setJobApplicationStatus", (JOB_ID, "sabotage", "somewhere"), "jsu", "setJobStatus"),
    ("export_setJobParameter", (JOB_ID, "name", "value"), "elasticJobParametersDB", "setJobParameter"),
    ("export_setJobParameters", (JOB_ID, [("name", "value")]), "elasticJobParametersDB", "setJobParameters"),
    ("export_sendHeartBeat", (JOB_ID, {}, {}), "jobDB", "setHeartBeatData"),
    ("export_updateJobFromStager", (JOB_ID, "Done"), "jsu", "setJobStatus"),
]


@pytest.mark.parametrize("method, args, target, targetMethod", MUTATORS)
def test_mutator_denied_for_other_user(otherUser, method, args, target, targetMethod):
    result = getattr(otherUser, method)(*args)
    assert not result["OK"]
    getattr(getattr(JobStateUpdateHandlerMixin, target), targetMethod).assert_not_called()


@pytest.mark.parametrize("method, args, target, targetMethod", MUTATORS)
def test_mutator_allowed_for_owner(owner, method, args, target, targetMethod):
    result = getattr(owner, method)(*args)
    assert result["OK"]
    getattr(getattr(JobStateUpdateHandlerMixin, target), targetMethod).assert_called_once()


@pytest.mark.parametrize("method, args, target, targetMethod", MUTATORS)
def test_mutator_allowed_for_pilot(pilot, method, args, target, targetMethod):
    """The JobAgent reports job status from the worker node with the pilot credential."""
    result = getattr(pilot, method)(*args)
    assert result["OK"]
    getattr(getattr(JobStateUpdateHandlerMixin, target), targetMethod).assert_called_once()


def test_pilot_cannot_set_immutable_attribute(pilot, jobDB):
    result = pilot.export_setJobAttribute(JOB_ID, "Owner", "somepilot")
    assert not result["OK"]
    jobDB.setJobAttribute.assert_not_called()


def test_job_sharing_group_member_is_allowed(makeHandler):
    handler = makeHandler("someotheruser", JOB_OWNER_GROUP, [Properties.NORMAL_USER, Properties.JOB_SHARING])
    assert handler.export_setJobStatus(JOB_ID, "Running", "", "Unknown")["OK"]


def test_same_group_without_job_sharing_is_denied(makeHandler):
    handler = makeHandler("someotheruser", JOB_OWNER_GROUP)
    assert not handler.export_setJobStatus(JOB_ID, "Failed", "", "Unknown")["OK"]


@pytest.mark.parametrize("prop", [Properties.JOB_ADMINISTRATOR, Properties.TRUSTED_HOST, Properties.OPERATOR])
def test_privileged_caller_can_modify_any_job(makeHandler, prop):
    handler = makeHandler("someservice", "some_group", [prop])
    assert handler.export_setJobStatus(JOB_ID, "Failed", "", "Unknown")["OK"]


def test_setJobsParameter_only_writes_authorized_jobs(makeHandler, jobDB):
    """A bulk update mixing owned and foreign jobs writes the owned ones and reports the refusal."""
    jobDB.getJobsAttributes.side_effect = lambda jobIDs, _attrs: S_OK(
        {
            int(jobID): {"Owner": JOB_OWNER if int(jobID) == 1 else "someoneelse", "OwnerGroup": JOB_OWNER_GROUP}
            for jobID in jobIDs
        }
    )
    handler = makeHandler()
    result = handler.export_setJobsParameter({1: ["name", "value"], 2: ["name", "value"]})
    assert not result["OK"]
    jobDB.getJobsAttributes.assert_called_once()
    setJobParameter = JobStateUpdateHandlerMixin.elasticJobParametersDB.setJobParameter
    setJobParameter.assert_called_once()
    assert setJobParameter.call_args.args[0] == 1


def test_setJobsParameter_allowed_for_owner(owner):
    result = owner.export_setJobsParameter({1: ["name", "value"], 2: ["name", "value"]})
    assert result["OK"]
    assert JobStateUpdateHandlerMixin.elasticJobParametersDB.setJobParameter.call_count == 2


# --------------------------------------------------------------------------- force flag


def test_force_ignored_for_non_administrator(owner):
    owner.export_setJobStatus(JOB_ID, "Done", "", "Unknown", force=True)
    assert JobStateUpdateHandlerMixin.jsu.setJobStatus.call_args.kwargs["force"] is False  # pylint: disable=no-member


def test_force_bulk_ignored_for_non_administrator(owner):
    owner.export_setJobStatusBulk(JOB_ID, {"2020-01-01 00:00:00": {"Status": "Done"}}, force=True)
    assert (
        JobStateUpdateHandlerMixin.jsu.setJobStatusBulk.call_args.kwargs["force"] is False  # pylint: disable=no-member
    )


def test_force_honoured_for_administrator(makeHandler):
    handler = makeHandler("admin", "admin_group", [Properties.JOB_ADMINISTRATOR])
    handler.export_setJobStatus(JOB_ID, "Done", "", "Unknown", force=True)
    assert JobStateUpdateHandlerMixin.jsu.setJobStatus.call_args.kwargs["force"] is True  # pylint: disable=no-member
