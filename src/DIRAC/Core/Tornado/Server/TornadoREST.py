"""
TornadoREST is the base class for your RESTful API handlers.
It directly inherits from :py:class:`tornado.web.RequestHandler`
"""

import os
import inspect
from tornado.escape import json_decode
from tornado.web import url as TornadoURL
from urllib.parse import unquote
from functools import partial

from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Tornado.Server.private.BaseRequestHandler import *

# decorator to determine the path to access the target method
location = partial(set_attribute, "location")
location.__doc__ = """
Use this decorator to determine the request path to the target method

Example:

    @location('/test/myAPI')
    def post_my_method(self, a, b):
        ''' Usage:

            requests.post(url + '/test/myAPI?a=value1?b=value2', cert=cert).context
            '["value1", "value2"]'
        '''
        return [a, b]
"""


class TornadoREST(BaseRequestHandler):  # pylint: disable=abstract-method
    """Base class for all the endpoints handlers.

    ### Example
    In order to create a handler for your service, it has to follow a certain skeleton.

    Simple example:

    .. code-block:: python

        from DIRAC.Core.Tornado.Server.TornadoREST import *

        class yourEndpointHandler(TornadoREST):

            def get_hello(self, *args, **kwargs):
                ''' Usage:

                        requests.get(url + '/hello/pos_arg1', params=params).json()['args]
                        ['pos_arg1']
                '''
                return {'args': args, 'kwargs': kwargs}

    .. code-block:: python

        from diraccfg import CFG
        from DIRAC.Core.Utilities.JDL import loadJDLAsCFG, dumpCFGAsJDL
        from DIRAC.Core.Tornado.Server.TornadoREST import *
        from DIRAC.WorkloadManagementSystem.Client.JobManagerClient import JobManagerClient
        from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient

        class yourEndpointHandler(TornadoREST):

            # Specify the default permission for the handler
            DEFAULT_AUTHORIZATION = ['authenticated']
            # Base URL
            DEFAULT_LOCATION = "/"

            @classmethod
            def initializeHandler(cls, infosDict):
                ''' Initialization '''
                cls.my_requests = 0
                cls.j_manager = JobManagerClient()
                cls.j_monitor = JobMonitoringClient()

            def initializeRequest(self):
                ''' Called at the beginning of each request '''
                self.my_requests += 1

            # In the annotation, you can specify the expected value type of the argument
            def get_job(self, jobID:int, category=None):
                '''Usage:

                    requests.get(f'https://myserver/job/{jobID}', cert=cert)
                    requests.get(f'https://myserver/job/{jobID}/owner', cert=cert)
                    requests.get(f'https://myserver/job/{jobID}/site', cert=cert)
                '''
                if not category:
                    return self.j_monitor.getJobStatus(jobID)
                if category == 'owner':
                    return self.j_monitor.getJobOwner(jobID)
                if category == 'owner':
                    return self.j_monitor.getJobSite(jobID)
                else:
                    # TornadoResponse allows you to call tornadoes methods, thread-safe
                    return TornadoResponse().redirect(f'/job/{jobID}')

            def get_jobs(self, owner=None, *, jobGroup=None, jobName=None):
                '''Usage:

                    requests.get(f'https://myserver/jobs', cert=cert)
                    requests.get(f'https://myserver/jobs/{owner}?jobGroup=job_group?jobName=job_name', cert=cert)
                '''
                conditions = {"Owner": owner or self.getRemoteCredentials}
                if jobGroup:
                    conditions["JobGroup"] = jobGroup
                if jobName:
                    conditions["JobName"] = jobName
                return self.j_monitor.getJobs(conditions, date)

            def post_job(self, manifest):
                '''Usage:

                    requests.post(f'https://myserver/job', cert=cert, json=[{Executable: "/bin/ls"}])
                '''
                jdl = dumpCFGAsJDL(CFG.CFG().loadFromDict(manifest))
                return self.j_manager.submitJob(str(jdl))

            def delete_job(self, jobIDs):
                '''Usage:

                    requests.delete(f'https://myserver/job', cert=cert, json=[123, 124])
                '''
                return self.j_manager.deleteJob(jobIDs)

            @authentication(["VISITOR"])
            @authorization(["all"])
            def options_job(self):
                '''Usage:

                    requests.options(f'https://myserver/job')
                '''
                return "You use OPTIONS method to access job manager API."

    .. note:: This example aims to show how access interfaces can be implemented and no more

    This class can read the method annotation to understand what type of argument expects to get the method,
    see :py:meth:`_getMethodArgs`.

    Note that because we inherit from :py:class:`tornado.web.RequestHandler`
    and we are running using executors, the methods you export cannot write
    back directly to the client. Please see inline comments in
    :py:class:`BaseRequestHandler <DIRAC.Core.Tornado.Server.private.BaseRequestHandler.BaseRequestHandler>` for more details.

    """

    # By default we enable all authorization grants, see DIRAC.Core.Tornado.Server.private.BaseRequestHandler for details
    DEFAULT_AUTHENTICATION = ["SSL", "JWT", "VISITOR"]
    METHOD_PREFIX = None
    DEFAULT_LOCATION = "/"

    @classmethod
    def _pre_initialize(cls) -> list:
        """This method is run by the Tornado server to prepare the handler for launch

        this method is run before the server tornado starts for each handler.

        it does the following:

            - searches for all possible methods for which you need to create routes
            - reads their annotation if present
            - adds attributes to each target method that help to significantly speed up
              the processing of the values of the target method arguments for each query
            - prepares mappings between URLs and handlers/method in a clear tornado format

        :returns: a list of URL (not the string with "https://..." but the tornado object)
                  see http://www.tornadoweb.org/en/stable/web.html#tornado.web.URLSpec
        """
        urls = []
        # Look for methods that are exported
        for prefix in [cls.METHOD_PREFIX] if cls.METHOD_PREFIX else cls.SUPPORTED_METHODS:
            prefix = prefix.lower()
            for mName, mObj in inspect.getmembers(cls, lambda x: callable(x) and x.__name__.startswith(prefix)):
                methodName = mName[len(prefix) :]
                cls.log.debug(f"  Find {mName} method")

                # Find target method URL
                url = os.path.join(
                    cls.DEFAULT_LOCATION, getattr(mObj, "location", "" if methodName == "index" else methodName)
                )
                if cls.BASE_URL and cls.BASE_URL.strip("/"):
                    url = cls.BASE_URL.strip("/") + (f"/{url}" if (url := url.strip("/")) else "")
                url = f"/{url.strip('/')}/?"

                cls.log.verbose(f" - Route {url} ->  {cls.__name__}.{mName}")

                # Discover positional arguments
                mObj.var_kwargs = False  # attribute indicating the presence of `**kwargs``

                args = []
                kwargs = {}
                # Read signature of a target function to explore arguments and their types
                # https://docs.python.org/3/library/inspect.html#inspect.Signature
                signature = inspect.signature(mObj)
                for name in list(signature.parameters)[1:]:  # skip `self` argument
                    # Consider in detail the description of the argument of the objective function
                    # to correctly form the route and determine the type of argument,
                    # see https://docs.python.org/3/library/inspect.html#inspect.Parameter
                    kind = signature.parameters[name].kind  # argument type
                    default = signature.parameters[name].default  # argument default value

                    # Determine what type of the target function argument is expected. By Default it's None.
                    _type = (
                        # Select the type specified in the target function, if any.
                        signature.parameters[name].annotation
                        if signature.parameters[name].annotation is not inspect.Parameter.empty
                        # If there is no argument annotation, take the default value type, if any
                        else type(default)
                        if default is not inspect.Parameter.empty and default is not None
                        # If you can not determine the type then leave None
                        else None
                    )

                    # Consider separately the positional arguments
                    if kind in [inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD]:
                        # register the positional argument type
                        args.append(_type)
                        # is argument optional
                        is_optional = (
                            kind is inspect.Parameter.POSITIONAL_OR_KEYWORD or default is inspect.Parameter.empty
                        )
                        # add to tornado route url regex describing the argument according to the type (if the type is specified)
                        # only simple types are considered, which should be more than enough
                        if _type is int:
                            url += r"(?:/([+-]?\d+)?)?" if is_optional else r"/([+-]?\d+)"
                        elif _type is float:
                            url += r"(?:/([+-]?\d*\.?\d+)?)?" if is_optional else r"/([+-]?\d*\.?\d+)"
                        elif _type is bool:
                            url += r"(?:/([01]|[A-z]+)?)?" if is_optional else r"/([01]|[A-z]+)"
                        else:
                            url += r"(?:/([\w%]+)?)?" if is_optional else r"/([\w%]+)"

                    # Consider separately the keyword arguments
                    if kind in [inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD]:
                        # register the keyword argument type
                        kwargs[name] = _type

                    if kind == inspect.Parameter.VAR_KEYWORD:
                        # if `**kwargs` is available in the target method,
                        # all additional query arguments will be passed there
                        mObj.var_kwargs = True
                        url += r"(?:[?&].+=.+)*"

                # We will leave the results of the study here so as not to waste time on each request
                mObj.keyword_kwarg_types = kwargs  # an attribute that contains types of keyword arguments
                mObj.positional_arg_types = args  # an attribute that contains types of positional arguments

                # We collect all generated tornado url for target handler methods
                if url not in urls:
                    cls.log.debug(f"  * {url}")
                    urls.append(TornadoURL(url, cls, dict(method=methodName)))
        return urls

    @classmethod
    def _getComponentInfoDict(cls, fullComponentName: str, fullURL: str) -> dict:
        """Fills the dictionary with information about the current component,

        :param fullComponentName: full component name, see :py:meth:`_getFullComponentName`
        :param fullURL: incoming request path
        """
        return {}

    @classmethod
    def _getCSAuthorizarionSection(cls, apiName):
        """Search endpoint auth section.

        :param str apiName: API name, see :py:meth:`_getFullComponentName`

        :return: str
        """
        return "%s/Authorization" % PathFinder.getAPISection(apiName)

    def _getMethod(self):
        """Get target method function to call. By default we read the first section in the path
        following the coincidence with the value of `DEFAULT_LOCATION`.
        If such a method is not defined, then try to use the `index` method.

        You can also restrict access to a specific method by adding a http method name as a target method prefix::

            # Available from any http method specified in SUPPORTED_METHODS class variable
            def export_myMethod(self, data):
                if self.request.method == 'POST':
                    # Do your "post job" here
                return data

            # Available only for POST http method if it specified in SUPPORTED_METHODS class variable
            def post_myMethod(self, data):
                # Do your "post job" here
                return data

        :return: function name
        """
        prefix = self.METHOD_PREFIX or f"{self.request.method.lower()}_"
        # the method key is appended to the URLSpec object when handling the handler in `_pre_initialize`,
        # the tornado server passes this argument to `initialize` method.
        # Read more about it https://www.tornadoweb.org/en/stable/web.html#tornado.web.RequestHandler.initialize
        return getattr(self, f"{prefix}{self._init_kwargs['method']}")

    def _getMethodArgs(self, args: tuple, kwargs: dict) -> tuple:
        """Search method arguments.

        By default, the arguments are taken from the description of the method itself.
        Then the arguments received in the request are assigned by the name of the method arguments.

        Usage:

            # requests.post(url + "/my_api/pos_only_value", data={'standard': standard_value, 'kwd_only': kwd_only_value}, ..
            # requests.post(url + "/my_api", json=[pos_only_value, standard_value, kwd_only_value], ..

            @location("/my_api")
            def post_note(self, pos_only, /, standard, *, kwd_only):
                ..


        .. warning:: this means that the target methods cannot be wrapped in the decorator,
                     or if so the decorator must duplicate the arguments and annotation of the target method

        :param args: positional arguments that comes from request path

        :return: target method args and kwargs
        """
        keywordArguments = {}
        positionalArguments = []

        for i, _type in enumerate(self.methodObj.positional_arg_types[: len(args)]):
            if arg := args[i]:
                positionalArguments.append(_type(unquote(arg)) if _type else unquote(arg))

        if self.request.headers.get("Content-Type") == "application/json":
            decoded = json_decode(body) if (body := self.request.body) else []
            return (positionalArguments + decoded, {}) if isinstance(decoded, list) else (positionalArguments, decoded)

        for name in self.request.arguments:
            if name in self.methodObj.keyword_kwarg_types or self.methodObj.var_kwargs:
                _type = self.methodObj.keyword_kwarg_types.get(name)
                # Get list of the arguments or on argument according to the type
                value = self.get_arguments(name) if _type in (tuple, list, set) else self.get_argument(name)
                # Wrap argument with annotated type
                keywordArguments[name] = _type(value) if _type else value

        return (positionalArguments, keywordArguments)
