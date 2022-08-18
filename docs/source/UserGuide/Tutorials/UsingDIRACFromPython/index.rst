.. _usingDIRACFromPython:

===============================
Using DIRAC Clients From Python
===============================

.. toctree::
   :maxdepth: 2

Overview
--------

For most tasks it is sufficient to use the DIRAC from a terminal via the commands that are prefixed with ``dirac-`` or via the WebApp.
In some cases however it becomes more convenient to use DIRAC directly from Python.

Before using the DIRAC's Python API it is useful to understand a few DIRAC-specific concepts.

.. _architecture:

Architecture
^^^^^^^^^^^^

DIRAC is built around a series of ``Systems`` which are independent components and are each contained in a submodule under the main ``DIRAC`` import.
Examples are:

* ``DataManagementSystem`` for accessing and managing data
* ``WorkloadManagementSystem`` for submitting and managing jobs

Each ``System`` contains one or more ``Clients`` which can be used for remotely requesting data from DIRAC services.
See :ref:`rpcCalls`.

Return values
^^^^^^^^^^^^^

Many DIRAC functions return either :py:func:`~DIRAC.Core.Utilities.ReturnValues.S_OK` or :py:func:`~DIRAC.Core.Utilities.ReturnValues.S_ERROR`.
In both of these cases the return value is a dictionary with an ``OK`` key which determines if the call was successful.

* :py:const:`True` the object corresponds to :py:func:`~DIRAC.Core.Utilities.ReturnValues.S_OK` and contains a ``Value`` key with the actual return value.
* :py:const:`False` the object corresponds to :py:func:`~DIRAC.Core.Utilities.ReturnValues.S_ERROR` and contains a ``Message`` key with a :py:class:`str` which should contain more information.

For example, DIRAC contains a utility function for looking up the IP addresses of domains that returns :py:func:`~DIRAC.Core.Utilities.ReturnValues.S_OK` or :py:func:`~DIRAC.Core.Utilities.ReturnValues.S_ERROR`.
This function can be used as follows:

.. code-block:: python

    from DIRAC.Core.Utilities.Network import getIPsForHostName

    for domain in ["diracgrid.org", "fake-domain.invalid"]:
        ret = getIPsForHostName(domain)
        if not ret["OK"]:
            raise RuntimeError(f"Failed to find IPs for {domain}.org with error: {ret['Message']}")
        ips = ret["Value"]
        print(f"{domain} is running at {ips}")

This will print:

.. code-block:: python

    diracgrid.org is running at ['134.158.16.209']
    Traceback (most recent call last):
    File "return-values-example.py", line 6, in <module>
        raise RuntimeError(f"Failed to find IPs for {domain}.org with error: {ret['Message']}")
    RuntimeError: Failed to find IPs for fake-domain.invalid.org with error: Can't get info for host fake-domain.invalid: [Errno 8] nodename nor servname provided, or not known

Often when writing user scripts it is useful to immediately raise an exception when an error happens and otherwise just return the value.
This is makes DIRAC behave more similarly to other Python functions and can be achieved using the helper function :py:func:`~DIRAC.Core.Utilities.ReturnValues.returnValueOrRaise`.
Rewriting our previous example for looking up IP addressed, this would be:

.. code-block:: python

    from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
    from DIRAC.Core.Utilities.Network import getIPsForHostName

    for domain in ["diracgrid.org", "fake-domain.invalid"]:
        ips = returnValueOrRaise(getIPsForHostName(domain))
        print(f"{domain} is running at {ips}")

.. _rpcCalls:

Remote Procedure Calls
^^^^^^^^^^^^^^^^^^^^^^

Remote Procedure Calls (RPC) describes the process of sending a request to another computer (i.e. DIRAC servers) that triggers them to process something.
The result of this remotely ran function is then sent back to the origin of the call (i.e. your computer).

In DIRAC this is done automatically when using the ``Client`` classes where calling a method ``MyClient().something(123)`` triggers the server to execute a function named ``export_something(123)``.
See :ref:`usingAClient` for an example of using a client.

Initializing DIRAC
------------------

Before using Python to contact a service DIRAC's global state needs to be initialized.
Most importantly, this process contacts the Configuration Service and starts a refresher thread.
When writing a Python script this should be done using the :py:func:`DIRAC.initialize` function.

.. code-block:: python

   import DIRAC
   DIRAC.initialize()

**Note** Currently it is essential to call :py:func:`~DIRAC.initialize` before importing clients.

**Note** If you're writing a ``dirac-`` command initialization should be handled differently. See :ref:`developingCommands`.

.. _usingAClient:

Using a client
--------------

Client classes can be found in the ``Client`` submodule inside each ``System`` module (see :ref:`architecture`).
For example to list job IDs by calling the ``whoami`` and ``getJobs`` RPC calls:

.. code-block:: python

    import DIRAC
    DIRAC.initialize()
    from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise
    from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient

    jmc = JobMonitoringClient()

    # Find out who the current user is
    my_details = returnValueOrRaise(jmc.whoami())
    username = my_details["username"]

    # Find the job IDs for the given user
    job_ids = returnValueOrRaise(jmc.getJobs({"owner": username}))

    print(f"Job IDs for {username} are: {job_ids}")
