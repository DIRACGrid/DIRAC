"""Regression tests for WorkflowReader parsing of workflow XML.

Non-string workflow parameters (e.g. a ``list`` or ``dict``) are serialised to
XML as ``repr()`` of their value and read back by evaluating that string. Such
values can legitimately exceed the ``saferEval`` default length cap, so the
reader must not impose it.

Regression: replacing ``eval`` with ``saferEval`` introduced a hard 2048-byte
limit, so parsing a workflow with a large non-string parameter failed with
``ValueError: Object string is too long (>2048 bytes)``.
"""

from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC.Core.Workflow.Workflow import Workflow, fromXMLString


def test_round_trip_large_list_parameter():
    # A list value whose repr() comfortably exceeds the 2048-byte default cap.
    big_list = [f"LFN:/lhcb/data/2026/RAW/file_{i:05d}.raw" for i in range(300)]
    assert len(str(big_list)) > 2048

    wf = Workflow()
    wf.setName("BigParamWF")
    wf.addParameter(Parameter("InputDataList", big_list, "list"))

    parsed = fromXMLString(wf.toXML())

    assert parsed.findParameter("InputDataList").getValue() == big_list
