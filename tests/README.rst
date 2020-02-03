.. -*- mode: rst -*-

Testing DIRAC
=============

These folders contain:

- Integration tests
- Workflow tests, which are a specific type of Integration tests
- System tests
- A few performance tests

but also:
- Some files holding configuration parameters for the test themselves
- Tools for running the tests above


These folder DON'T contain the unit tests, which are instead to be found inside the "code" directories (e.g. /DIRAC/WorkloadManagementSystem/Agent/test contains the unit tests for the code of /DIRAC/WorkloadManagementSystem/Agent/*.py)
