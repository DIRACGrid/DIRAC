""" This is a test of the PublisherHandler

    It supposes that the RSS DBs are present, and that the service is running
"""
# pylint: disable=wrong-import-position

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Client.PublisherClient import PublisherClient

publisher = PublisherClient()
gLogger.setLevel("DEBUG")


def test_Get(serverIsOlderThan):
    res = publisher.getSites()
    assert res["OK"] is True, res["Message"]

    res = publisher.getSitesResources(None)
    assert res["OK"] is True, res["Message"]

    res = publisher.getElementStatuses("Site", None, None, None, None, None)
    assert res["OK"] is True, res["Message"]

    res = publisher.getElementHistory("Site", None, None, None)
    assert res["OK"] is True, res["Message"]

    res = publisher.getElementPolicies("Site", None, None)
    assert res["OK"] is True, res["Message"]

    # Nodes (queues) were dropped from the RSS, and getNodeStatuses with them, in v9.1
    if serverIsOlderThan(publisher, "9.1"):
        res = publisher.getNodeStatuses()
        assert res["OK"] is True, res["Message"]

    res = publisher.getTree("", "")
    assert res["OK"] is True, res["Message"]
