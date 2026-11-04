======================================
Developing Future Clients
======================================

This is a step by step guide to develop proper DIRAC *FutureClients* for a
seemless DIRAC-DiracX compatibility.

A ``FutureClient`` is a replacement of a legacy DIRAC ``Client`` that is capable of
talking to DiracX in a transparent way for the user.

.. contents::
   :local:
   :depth: 1


Overview
========

DIRAC and DiracX access points are organized differently:

* **DIRAC** exposes a service through a **Handler** (the implementation) and a
  **Client** (the access point).
* **DiracX** has no handlers. It has an **API** that uses the ``logic`` through
  the ``routers``.

The call chain of a migrated service is:

.. code-block:: text

  Client (legacy) -> FutureClient -> DiracX API -> DiracX router -> DiracX logic

For ease of understanding, a simple made up service will be used as examples:

.. code-block:: text

   DIRAC
   └── src/DIRAC/WorkloadManagementSystem/
       ├── Service/JobAnnotatorHandler.py
       ├── Client/JobAnnotatorClient.py
       └── FutureClient/JobAnnotatorClient.py

   diracx
   ├── diracx-routers/src/diracx/routers/jobs/annotations.py   # API: HTTP endpoints
   └── diracx-logic/src/diracx/logic/jobs/annotations.py       # API: business logic


Step 1: Understand requirements
===============================

Before writing any code, find out exactly what the legacy service does.

DIRAC exposes a service through two parts:

* The **Handler**, located in ``Service/<Name>Handler.py``, which contains the
  real implementation. Each remotely callable method is called
  ``export_<method>`` and its accepted argument types are declared in
  ``types_<method>`` right above.
* The **Client**, located in ``Client/<Name>Client.py``, is almost always
  empty and only has a decorator that points to the Handler.

In this example, ``WorkloadManagement/JobAnnotator`` points to
``WorkloadManagementSystem/Service/JobAnnotatorHandler.py``.

The made up legacy Handler:

.. literalinclude:: DIRACHandler.py
   :language: python
   :linenos:

And the legacy Client:

.. literalinclude:: DIRACClient.py
   :language: python
   :linenos:

Write down, for every ``export_*`` method of the Handler, what parameters it
receives and what it returns.

While developing the ``FutureClient``, the code
might differ a lot, but the inputs and outputs **MUST** stay exactly the same as the
one in the legacy client.

Step 2: Check current DiracX API
=======================================

Now check whether DiracX already provides everything you need. Look at the
routers of the DiracX API, in
``diracx-routers/src/diracx/routers/<group>/``. A group is built in its
``__init__.py`` by including several routers. For example, the ``jobs`` group
looks like this:

.. code-block:: python

   # diracx-routers/src/diracx/routers/jobs/__init__.py
   # Router definition
   router = DiracxRouter()
   router.include_router(sandboxes_router)
   router.include_router(status_router)
   router.include_router(query_router)
   router.include_router(submission_router)

Every endpoint of those routers is available in the DiracX API as
``api.<group>.<function_name>``. For example, a router function:

.. code-block:: python

   @router.patch("/something")
   async def do_something(...): ...

will be called from DIRAC as:

.. code-block:: python

   with DiracXClient() as api:
       api.jobs.do_something(...)

Try to map each legacy method to a DiracX operation. The mapping is **NOT** always
one to one:

* DiracX endpoints are modern and might mix or split DIRAC handler's methods.
* The arguments usually need to be reshaped. For example, there are DiracX endpoints
  that take a dictionary keyed by ``jobID``, so many jobs can be updated at once,
  something that DIRAC doesn't usually do.
* The returned value can be different and may need to be converted back to the
  legacy format.

Then decide if every legacy method can be mapped or can use a combination of APIs. If so, then **skip Step 3**
and go directly to Step 4.
If something is missing, continue with Step 3.

For the ``JobAnnotator`` example, since it's made up, there is nothing about it
in any of the existing routers, so we need Step 3.

Step 3: Develop new DiracX API
=====================================

The API is split in two layers:

* **logic** (``diracx-logic``): the business logic, independent of HTTP.
* **routers** (``diracx-routers``): the HTTP endpoints. They check
  permissions, call the logic and translate exceptions into HTTP errors.

3.1 Where does the new code go?
-------------------------------

In this example the annotations belong to jobs, so the new code goes into the
existing ``jobs`` group of DiracX:

.. code-block:: text

   diracx/
   ├── diracx-logic/src/diracx/logic/jobs/annotations.py
   └── diracx-routers/src/diracx/routers/jobs/annotations.py

Keep both layers in the same group directory (``jobs``) and with the same file
name (``annotations.py``).

.. important::

   ``jobs`` is only the right place **for this example**. For other
   developments, the new code might need to go to another existing directory
   (such as pilots or auth), or even to a new group directory.

   Keep in mind that the group is also the namespace of the DiracX
   client: code placed in ``routers/<group>/`` is called from DIRAC as
   ``api.<group>.<operation>``, so choose the group name carefully,
   as it becomes part of the public API.

3.2 Logic
---------

.. literalinclude:: DiracXLogic.py
   :language: python
   :linenos:


3.3 Router
----------

.. literalinclude:: DiracXRouter.py
   :language: python
   :linenos:

Then register the new router in the ``jobs`` group, next to the existing ones:

.. code-block:: python

   # diracx-routers/src/diracx/routers/jobs/__init__.py
   # (other imports)
   from .annotations import router as annotations_router

   # Router definition
   router = DiracxRouter()
   router.include_router(sandboxes_router)
   router.include_router(status_router)
   router.include_router(query_router)
   router.include_router(submission_router)
   router.include_router(annotations_router)   # <-- new

Because ``annotations_router`` is included in the ``jobs`` router, its
endpoints end up in the ``jobs`` group of the DiracX API.

3.4 DiracX API
-----------------

There is no code to write here. The DiracX API will need to be generated from the routers.

Once your DiracX change is merged, the new endpoints appear in the
client with the name of the router function, inside the group of the router:

======================  ======================================  ==============================
Router function         Endpoint                                DiracX API call
======================  ======================================  ==============================
``add_annotation``      ``POST /api/jobs/annotations``          ``api.jobs.add_annotation``
``get_annotations``     ``GET /api/jobs/{job_id}/annotations``  ``api.jobs.get_annotations``
======================  ======================================  ==============================

Step 4: Code the new Future Client in DIRAC
===========================================

Create the FutureClient in the ``FutureClient`` directory of the same
system:

.. code-block:: text

   DIRAC/WorkloadManagementSystem/FutureClient/JobAnnotatorClient.py

A FutureClient:

* Inherits from ``FutureClient``.
* Implements **the same methods, with the same arguments** as the legacy
  service.
* Calls DiracX through the ``DiracXClient`` context manager.

.. literalinclude:: DIRACFutureClient.py
   :language: python
   :linenos:

Step 5: Change the DIRAC Client configuration
=============================================

The legacy Client requires just a minimal change in its configuration
for it to see the new changes:

.. literalinclude:: DIRACClientNew.py
   :language: python
   :linenos:

Now the FutureClient is ready to roll into production. Yay :)

Disabling the DiracX API
---------------------------

The DiracX API can be disabled for a given service from the web interface
(the Configuration System), so DIRAC keeps using the legacy Client and Handler
for it. The option is located under ``DiracX/legacyClientEnabled``.

For this example, to disable the use of the ``FutureClient``, the config would
be as follows:

.. code-block:: text

   DiracX
   └── legacyClientEnabled
       └── WorkloadManagement
           └── JobAnnotator = yes
