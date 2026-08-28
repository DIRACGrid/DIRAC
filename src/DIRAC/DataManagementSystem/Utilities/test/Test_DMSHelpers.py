"""DMSHelpers is a per-VO singleton holding method caches guarded by locks.

Copies and pickles must resolve back to the shared instance: duplicating it
would fork the caches, and serialising it is impossible anyway (locks). The
regression pinned here: deepcopying an object that embeds a DMSHelpers —
e.g. an RMS Request — raised "cannot pickle '_thread.lock' object".
"""

import copy
import pickle

from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers


def test_copy_and_deepcopy_preserve_the_singleton():
    helper = DMSHelpers()
    assert copy.copy(helper) is helper
    assert copy.deepcopy(helper) is helper


def test_pickle_roundtrip_resolves_to_the_singleton():
    helper = DMSHelpers()
    assert pickle.loads(pickle.dumps(helper)) is helper


def test_objects_embedding_a_helper_can_be_deepcopied():
    request_like = {"payload": [1, 2], "dmsHelper": DMSHelpers()}
    clone = copy.deepcopy(request_like)
    assert clone["dmsHelper"] is request_like["dmsHelper"]
    assert clone["payload"] == [1, 2] and clone["payload"] is not request_like["payload"]


def test_request_deepcopy_regression():
    # The in-the-wild breakage: Request.__init__ stores a DMSHelpers
    from DIRAC.RequestManagementSystem.Client.Request import Request

    request = Request()
    clone = copy.deepcopy(request)
    assert clone.dmsHelper is request.dmsHelper
