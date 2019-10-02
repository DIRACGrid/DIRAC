.. _installing-configuring-basics:

Installing and Configuring: Basic Concepts
==========================================

As seen in :ref:`dirac-setup-structure`, DIRAC provides you with several *components*,
these components are organized in *systems*, and these components can be installed in a :ref:`server_installation`
using the :ref:`system-admin-console`.

The components don't need to be all resident on the same host, in fact it's common practice to have several hosts
for large installations.

Normally, services are always exposed on the same port, which is defined in the configuration for each of them.

As a general rule, services can be duplicated,
meaning you can have the same service running on multiple hosts, thus reducing the load.
There are only 2 cases of DIRAC services that have a "master/slave" concept, and these are the Configuration Service
and the Accounting/DataStore service.
The WorkloadManagement/Matcher service should also not be duplicated.

Same can be said for executors: you can have many residing on different hosts.

The same can't be said for agents. Some of them can be duplicated, BUT require a proper configuration,
and for this you need to read further in the guide (See `scalingLimitations`).


Each component has a configuration
----------------------------------

When you install a component, it comes with a default configuration.
The configuration is available to all the components via the Configuration Service,
and its content is exposed by the Configuration Service WebApp in the DIRAC web portal.

The next section, :ref:`dirac-configuration` keeps a reference of the configuration for each of the components.
You don't need to read it all now, you just need to know it's there.


What to install
---------------

It depends!

Some components will be needed, whatever you do, e.g. as it should be clear already,
you will need always the Configuration Service.

And almost certainly, a large part of what is part of DIRAC framework (the FrameworkSystem) is needed.

Then, it depends from what you want to do. So, if you just want to run some jobs,
you'd need to install WorkloadManagementSystem components.
If you need to do something else... then, again, it depends.

You need to keep reading.
