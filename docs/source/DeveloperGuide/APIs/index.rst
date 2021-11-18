.. _apis:

Systems APIs
=============

Currently, client-server interaction with DIRAC systems is provided by the HTTP services that are coded using the :py:class:`~DIRAC.Core.Tornado.Server.TornadoService` module.
This module implements a fixed interface via HTTP POST requests meant to implement RPC calls, see :ref:`httpsTornado`.
Starting with 8.0, an additional :py:class:`~DIRAC.Core.Tornado.Server.TornadoREST` module was created, which expands the possibilities for writing access interfaces to DIRAC systems by implementing a REST interface.

``TornadoService`` and ``TornadoREST`` inherit from :py:class:`~DIRAC.Core.Tornado.Server.private.BaseRequestHandler` class the basic logic for authorizing the request, search and run the target method in the thread.

.. image:: /_static/Systems/APIs/APIClass.png
   :width: 400px
   :alt: Inheritance of classes.
   :align: center

From the scheme it is intuitively clear that on the basis of BaseRequestHandler it is possible to implement other frameworks.

What is the purpose of this?
----------------------------

The purpose of this component is to build DIRAC system status access interfaces based on HTTP requests outside the fixed RPC request interface described in TornadoServices.

The main reason for the implementation of this feature is the need to implementation of API for DIRAC Authorization Server based on OAuth2 framework,
which in turn is an integral part of the implementation of OAuth2 DIRAC authorization.
So currently, there is only one API implementation for the Framework system, which is :py:class:`~DIRAC.FrameworkSystem.API.AuthHandler`.
For the same reason, the ability to authorize with an access token (see https://datatracker.ietf.org/doc/html/rfc6750#section-2) was added to the authorization steps.

How to write APIs
-----------------

You need to choose the system for which you want to write an API, then create a file with the name of your API in the following path /src/DIRAC/<System name>/API/<Your API name>Handler.py

.. literalinclude:: /../../src/DIRAC/Core/Tornado/Server/TornadoREST.py
    :start-after: ### Example
    :end-before: """
    :caption: Example of the simple system API handler

.. note:: If you need to implement the interface on a standard 443 https port, you will need to use a balancer, such as nginx

The example described is likely to be sufficient for most writing cases. But here are some additional features, see :py:class:`~DIRAC.Core.Tornado.Server.private.BaseRequestHandler`:

  - ``USE_AUTHZ_GRANTS`` set the list and order of steps to authorize the request. For example, set ``USE_AUTHZ_GRANTS = ["JWT"]`` to allow access to your endpoint only with a valid access token.
  - ``AUTH_PROPS`` set the authorization requirements. For example, ``AUTH_PROPS = ['authenticated']`` will allow access only to authenticated users.
  - in addition to standard S_OK/S_ERROR you can return text, whether the dictionary for example or nothing, the result will be sent with a 200 status.
  - ``path_<my_method>`` will allow you to consider the path parameters as positional arguments of the target method. For example:

    .. code-block:: python

        # It's allow make request like a GET /user/contacts/Bob
        path_user = ["(contacts|IDs)", "([A-z%0-9-_]+)"]

        def web_user(self, option:str, name:str):
            return Registry.getUserOption(name, option)

  - If your API is complex enough and may include, for example, redirection or additional headers, you can use :py:class:`~DIRAC.Core.Tornado.Server.private.BaseRequestHandler.TornadoResponse`
    to add all these necessary things, which is thread-safe because TornadoResponse will call your actions outside the thread in which this method is executed:

    .. code-block:: python

        from DIRAC.Core.Tornado.Server.private.BaseRequestHandler import TornadoResponse

        class MyClass(TornadoREST):

            # Describe the class as in the example.

            def web_myMethod(self, my_option):
                # Do some thing
                response = TornadoResponse(data)
                response.set_status(201)
                return response


The framework takes into account the target method annotations when preparing arguments using the ``inspect`` module, see https://docs.python.org/3/library/inspect.html#inspect.signature.
This means that if you specify an argument type in the annotation, the framework will try to convert the received argument from the request to the specified type.

.. note:: This component is still quite poor and can be improved by subsequent PRs.
