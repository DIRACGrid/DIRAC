.. _testing_agents:

=====================================
Testing an agent while developing it
=====================================

An agent can be tested with a unit test, e.g.::

.. code-block:: python

   import pytest
   from mock import MagicMock

   from DIRAC.MySystem.Agent.MyAgent import MyAgent

   mockAM = MagicMock()

   def test_myTest(mocker):
       mocker.patch("DIRAC.MySystem.Agent.MyAgent.AgentModule", side_effect=mockAM)
       ma = MyAgent
       ma.something()
