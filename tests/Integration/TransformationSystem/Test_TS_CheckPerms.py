#!/usr/bin/env python
""" This tests the TransformationManager checkPermission function
"""
import unittest
import functools
import sys

import DIRAC
from DIRAC import S_OK, S_ERROR

DIRAC.initialize()  # Initialize configuration

from DIRAC.TransformationSystem.Service.TransformationManagerHandler import TransformationManagerHandlerMixin


class TSCheckPermTestCase(unittest.TestCase):
    TEST_USERS = {
        # Users a1/a2 are just regular tf users with no sharing
        # a1 & a2 are in the same group
        "user_a1": {"username": "user_a1", "group": "group_a", "properties": ["ProductionUser"]},
        "user_a2": {"username": "user_a2", "group": "group_a", "properties": ["ProductionUser"]},
        # Users b1/b2 have production sharing for their group
        # b1 & b2 are in the same group
        "user_b1": {"username": "user_b1", "group": "group_b", "properties": ["ProductionSharing"]},
        "user_b2": {"username": "user_b2", "group": "group_b", "properties": ["ProductionSharing"]},
        # Admin user has full ProductionManagement permission
        "admin": {"username": "admin", "group": "dirac_admin", "properties": ["ProductionManagement"]},
        # Vistor has no tf permissions at all
        "visitor": {"username": "none", "group": "visitors", "properties": []},
    }
    TEST_TRANSFORMATIONS = {
        # Each transformation is owned by the matching user
        # e.g. user_a1 owns TF_A1 and so forth...
        "TF_A1": {"Author": "user_a1", "AuthorGroup": "group_a"},
        "TF_A2": {"Author": "user_a2", "AuthorGroup": "group_a"},
        "TF_B1": {"Author": "user_b1", "AuthorGroup": "group_b"},
        "TF_B2": {"Author": "user_b2", "AuthorGroup": "group_b"},
        "TF_C1": {"Author": "user_c1", "AuthorGroup": "group_c"},
    }
    TEST_PERM_MATRIX = (
        # User a1 should only be able to access their own tf
        ("user_a1", "TF_A1", True),
        ("user_a1", "TF_A2", False),
        ("user_a1", "TF_B1", False),
        ("user_a1", "TF_B2", False),
        ("user_a1", "TF_C1", False),
        # User a2 should only be able to access their own tf
        ("user_a2", "TF_A1", False),
        ("user_a2", "TF_A2", True),
        ("user_a2", "TF_B1", False),
        ("user_a2", "TF_B2", False),
        ("user_a2", "TF_C1", False),
        # User b1 should have access to all in the B group
        ("user_b1", "TF_A1", False),
        ("user_b1", "TF_A2", False),
        ("user_b1", "TF_B1", True),
        ("user_b1", "TF_B2", True),
        ("user_b1", "TF_C1", False),
        # User b2 should have access to all in the B group
        ("user_b2", "TF_A1", False),
        ("user_b2", "TF_A2", False),
        ("user_b2", "TF_B1", True),
        ("user_b2", "TF_B2", True),
        ("user_b2", "TF_C1", False),
        # Admin should have access to all
        ("admin", "TF_A1", True),
        ("admin", "TF_A2", True),
        ("admin", "TF_B1", True),
        ("admin", "TF_B2", True),
        ("admin", "TF_C1", True),
        # Vistor should have access to none
        ("visitor", "TF_A1", False),
        ("visitor", "TF_A2", False),
        ("visitor", "TF_B1", False),
        ("visitor", "TF_B2", False),
        ("visitor", "TF_C1", False),
        # Only admin should have access to a non-existant tfName
        # (as admin is a short-circuit check that doesn't actually go to the DB)
        ("user_a1", "TF_ZZ", False),
        ("user_a2", "TF_ZZ", False),
        ("user_b1", "TF_ZZ", False),
        ("user_b2", "TF_ZZ", False),
        ("admin", "TF_ZZ", True),
        ("visitor", "TF_ZZ", False),
    )

    class _mockTFDatabase:
        """A class that mocks up the Transformation database."""

        def getTransformation(self, tfName):
            if tfName in TSCheckPermTestCase.TEST_TRANSFORMATIONS:
                return S_OK(TSCheckPermTestCase.TEST_TRANSFORMATIONS[tfName])
            return S_ERROR(f"Failed to find transformation {tfName}")

    def _mockGetRemoteCredentials(self, tfmHandler):
        """Mocks up get remote credentails based on the current user."""
        return self.currentUser

    def _setUser(self, user):
        """Sets the current user for the mocked getRemoteCredentials function from the test dict."""
        if user in self.TEST_USERS:
            self.currentUser = self.TEST_USERS[user]
            return
        self.currentUser = self.TEST_USERS["visitor"]

    def setUp(self):
        """Create TransformationManagerHandlerMixin and then replace the key functions directly
        with the mocked up ones.
        """
        self.handler = TransformationManagerHandlerMixin()
        self.handler.getRemoteCredentials = functools.partial(self._mockGetRemoteCredentials, self)
        self.handler.transformationDB = self._mockTFDatabase()
        self._setUser(None)

    def test_checkPerms(self):
        for user, tfName, expRes in self.TEST_PERM_MATRIX:
            self._setUser(user)
            res = self.handler.checkPermissions(tfName)
            self.assertEqual(
                expRes, res["OK"], f"User {user} access to tf {tfName}: {res['OK']} but expected {expRes}."
            )


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TSCheckPermTestCase)
    testResult = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not testResult.wasSuccessful())
