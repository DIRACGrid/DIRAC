======================================
Developing Future Clients
======================================

This is a step by step guide to develop proper DIRAC *FutureClients* for a seamless DIRAC-DiracX compatibility.

A ``FutureClient`` is a replacement of a legacy DIRAC ``Client`` that is capable of talking to DiracX in a transparent
way for the user.

.. contents::
   :local:
   :depth: 1


Overview
========

DIRAC and DiracX access points are organized differently:

* **DIRAC** exposes a service through a **Handler** (the implementation) and a **Client** (the access point).
* **DiracX** has no handlers. It has an **API** that uses the ``logic`` through the ``routers``.

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

* The **Handler**, located in ``Service/<Name>Handler.py``, which contains the real implementation. Each remotely
  callable method is called ``export_<method>`` and its accepted argument types are declared in ``types_<method>`` right
  above.
* The **Client**, located in ``Client/<Name>Client.py``, is almost always empty and only has a decorator that points to
  the Handler.

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

Write down, for every ``export_*`` method of the Handler, what parameters it receives and what it returns.

While developing the ``FutureClient``, the code might differ a lot, but the inputs and outputs **MUST** stay exactly the
same as the one in the legacy client. The only accepted exception is the unused return value of methods that change
something, see ``@stripValueIfOK`` in Step 4.

Step 2: Check current DiracX API
=======================================

Now check whether DiracX already provides everything you need. Look at the routers of the DiracX API, in
``diracx-routers/src/diracx/routers/<group>/``. A group is built in its ``__init__.py`` by including several routers.
For example, the ``jobs`` group looks like this:

.. code-block:: python

   # diracx-routers/src/diracx/routers/jobs/__init__.py
   # Router definition
   router = DiracxRouter()
   router.include_router(sandboxes_router)
   router.include_router(status_router)
   router.include_router(query_router)
   router.include_router(submission_router)

Every endpoint of those routers is available in the DiracX API as ``api.<group>.<function_name>``. For example, a router
function:

.. code-block:: python

   @router.patch("/something")
   async def do_something(...): ...

will be called from DIRAC as:

.. code-block:: python

   with DiracXClient() as api:
       api.jobs.do_something(...)

Try to map each legacy method to a DiracX operation. The mapping is **NOT** always one to one:

* DiracX endpoints are modern and might mix or split DIRAC handler's methods.
* The arguments usually need to be reshaped. For example, there are DiracX endpoints that take a dictionary keyed by
  ``jobID``, so many jobs can be updated at once, something that DIRAC doesn't usually do.
* The returned value can be different and may need to be converted back to the legacy format.

Then decide if every legacy method can be mapped or can use a combination of APIs. If so, then **skip Step 3** and go
directly to Step 4. If something is missing, continue with Step 3.

For the ``JobAnnotator`` example, since it's made up, there is nothing about it in any of the existing routers, so we
need Step 3.

Step 3: Develop new DiracX API
=====================================

The API is split in two layers:

* **logic** (``diracx-logic``): the business logic, independent of HTTP.
* **routers** (``diracx-routers``): the HTTP endpoints. They check permissions, call the logic and translate exceptions
  into HTTP errors.

3.1 Where does the new code go?
-------------------------------

In this example the annotations belong to jobs, so the new code goes into the existing ``jobs`` group of DiracX:

.. code-block:: text

   diracx/
   ├── diracx-logic/src/diracx/logic/jobs/annotations.py
   └── diracx-routers/src/diracx/routers/jobs/annotations.py

Keep both layers in the same group directory (``jobs``) and with the same file name (``annotations.py``).

.. important::

   ``jobs`` is only the right place **for this example**. For other developments, the new code might need to go to
   another existing directory (such as pilots or auth), or even to a new group directory.

   Keep in mind that the group is also the namespace of the DiracX client: code placed in ``routers/<group>/`` is called
   from DIRAC as ``api.<group>.<operation>``, so choose the group name carefully, as it becomes part of the public API.

3.2 Logic
---------

The logic layer is made of plain async functions. They receive the DB objects as arguments and know nothing about HTTP.

.. literalinclude:: DiracXLogic.py
   :language: python
   :linenos:

A few things to notice:

* The endpoint takes many jobs at once, so the logic checks **all** of them before writing anything. A missing job must
  not leave a partial write behind.
* It talks to the database with one query per call, not one per job.
* A missing job raises ``JobNotFoundError``, the same exception used by the existing ``jobs`` logic.

.. note::

   ``JobDB.insert_annotations`` and ``JobDB.get_annotations`` do not exist: they stand for whatever DB methods your
   feature needs. New tables and DB methods go into ``diracx-db``, which is not covered by this guide.

3.3 Router
----------

The router handles HTTP: it checks permissions, calls the logic and turns the logic exceptions into HTTP errors.

.. literalinclude:: DiracXRouter.py
   :language: python
   :linenos:

* **Permissions**: every route must call an access policy (here ``check_permissions`` with the ``ActionType`` matching
  the operation) or be explicitly marked ``@open_access``. The DiracX test suite fails if a route does neither. Without
  it, any authenticated user could read or change any job.
* **Errors**: ``JobNotFoundError`` becomes ``404 NOT FOUND``, as in the other ``jobs`` routes. On the DIRAC side, the
  FutureClient turns it into an ``S_ERROR``.

.. tip::

   Once the shape of the endpoint is clear, the body of the logic and the router can be drafted with an AI coding
   assistant. Which one is up to you: by the time you read this, there is probably a newer one. Whichever it is, it will
   sound very sure of itself, and so will its magic lines. Review what it produces against the existing routers in the
   same group (permission checks, error mapping, number of DB queries), as you would for any other contribution. The
   reviewer is still a human, for now.

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

Because ``annotations_router`` is included in the ``jobs`` router, its endpoints end up in the ``jobs`` group of the
DiracX API.

3.4 DiracX API
-----------------

There is no code to write by hand here: the DiracX client is generated from the routers. The new endpoints appear in the
client with the name of the router function, inside the group of the router:

======================  ======================================  ==============================
Router function         Endpoint                                DiracX API call
======================  ======================================  ==============================
``add_annotations``     ``POST /api/jobs/annotations``          ``api.jobs.add_annotations``
``get_annotations``     ``GET /api/jobs/{job_id}/annotations``  ``api.jobs.get_annotations``
======================  ======================================  ==============================

For DIRAC to be able to call them, several things have to happen, in this order:

#. **Regenerate the client in the same DiracX pull request** as the router (see the DiracX developer documentation for
   the ``generate-client`` command). The generated code is committed, so the PR is incomplete without it.
#. **Wait for a DiracX release** that contains the change.
#. **Bump the DiracX version required by DIRAC** to that release. Until then, ``api.jobs.add_annotations`` does not
   exist in the client DIRAC installs, and calling it fails with an ``AttributeError``.
#. **Keep the server in mind**: the FutureClient only works against a DiracX server that is at least that version. An
   older server answers ``404 NOT FOUND`` for the new endpoint, so the FutureClient must not be enabled (see `Step 6:
   Enable the FutureClient`_) on an installation before its DiracX server has been upgraded.

Step 4: Code the new Future Client in DIRAC
===========================================

Create the FutureClient in the ``FutureClient`` directory of the same system:

.. code-block:: text

   DIRAC/WorkloadManagementSystem/FutureClient/JobAnnotatorClient.py

A FutureClient:

* Inherits from ``FutureClient``.
* Implements **the same methods, with the same arguments** as the legacy service.
* Calls DiracX through the ``DiracXClient`` context manager.

.. literalinclude:: DIRACFutureClient.py
   :language: python
   :linenos:

* ``@convertToReturnValue`` turns the return value into ``S_OK(value)`` and any exception (including HTTP errors from
  DiracX) into ``S_ERROR``, so callers get the usual DIRAC return structure.
* ``@addRPCStub`` goes on methods that **change** something (here ``addAnnotation``). It attaches an ``rpcStub`` to the
  result, like a DISET call does. If the call fails, the caller can put this stub into a ``ForwardDISET`` request of the
  Request Management System, which replays the call later. Methods that only read (here ``getAnnotations``) do not need
  it.
* ``@stripValueIfOK`` also goes on methods that change something. Legacy handlers often return a value nobody uses for
  writes (here the result of ``JobAnnotatorDB.addAnnotation``, often a number of modified rows), while the DiracX
  endpoint returns nothing. Instead of returning ``S_OK(None)``, the decorator removes the ``Value`` key, so a caller
  that does rely on the legacy value fails with a clear ``KeyError`` instead of silently getting ``None``. If a caller
  really needs the value, return it from the FutureClient instead.

Step 5: Plug the FutureClient into the legacy Client
====================================================

The legacy Client needs one change: a ``diracxClient`` attribute that points to the FutureClient (highlighted lines):

.. code-block:: python
   :linenos:
   :emphasize-lines: 2-4,11

   # DIRAC/WorkloadManagementSystem/Client/JobAnnotatorClient.py
   from DIRAC.WorkloadManagementSystem.FutureClient.JobAnnotatorClient import (
       JobAnnotatorClient as futureJobAnnotatorClient,
   )

   from DIRAC.Core.Base.Client import Client, createClient


   @createClient("WorkloadManagement/JobAnnotator")
   class JobAnnotatorClient(Client):
       diracxClient = futureJobAnnotatorClient

       def __init__(self, url=None, **kwargs):
           super().__init__(**kwargs)
           self.serverURL = url or "WorkloadManagement/JobAnnotator"

This change alone does **not** switch anything to DiracX: the legacy Client keeps calling the DIRAC Handler until the
FutureClient is enabled in the Configuration System.

Step 6: Enable the FutureClient
===============================

Whether a Client uses the FutureClient or the legacy Handler is decided per service by the Configuration System option
``/DiracX/FutureClientEnabled/<System>/<Service>``:

* ``yes`` (or ``true``, ``y``, ``1``): the Client calls DiracX through the FutureClient.
* ``no``, or option not set (the default): the Client calls the legacy DIRAC Handler.

For this example, to enable the ``FutureClient``:

.. code-block:: text

   DiracX
   {
     FutureClientEnabled
     {
       WorkloadManagement
       {
         JobAnnotator = yes
       }
     }
   }

To go back to the legacy Handler, set the option to ``no`` or remove it.

.. warning::

   If the option is ``yes`` but the legacy Client has no ``diracxClient`` attribute (Step 5), every call made through
   the Client raises ``NotImplementedError``. Enable the option only on installations running a DIRAC version that
   contains Step 5, and a DiracX server that contains Step 3.
