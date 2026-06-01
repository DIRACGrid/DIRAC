"""
  Defines the plugin to perform evaluation on the lfn name
"""
import ast

from DIRAC.Resources.Catalog.ConditionPlugins.FCConditionBasePlugin import FCConditionBasePlugin


class FilenamePlugin(FCConditionBasePlugin):
    """
    This plugin is to be used when filtering based on the LFN name
    """

    SUPPORTED_METHODS = frozenset(
        {
            "startswith",
            "endswith",
            "isalnum",
            "isalpha",
            "isdigit",
            "islower",
            "isspace",
            "istitle",
            "isupper",
            "find",
        }
    )

    @staticmethod
    def _parseFn(expr):
        """This function takes a condition string (which is a python function call)
        and extracts the funtion name, args and kwargs.

        Raises ValueError if the expression isn't valid.
        """
        if len(expr) > 128:
            raise ValueError("Expression must be under 128 chars")
        try:
            tree = ast.parse(expr, mode="eval")
        except SyntaxError:
            raise ValueError("Invalid syntax in expression")

        # Check the outer part is a plain function name
        node = tree.body
        if not isinstance(node, ast.Call):
            raise ValueError("Expected a function expression")
        func = node.func
        if not isinstance(func, ast.Name):
            raise ValueError("Expected a function name")
        # Extract the name and parameters
        fnName = func.id
        try:
            args = []
            for arg in node.args:
                args.append(arg.value)
            kwargs = {kw.arg: kw.value.value for kw in node.keywords if kw.arg is not None}
        except Exception:
            # If the parameters to the function are no constant, we'll get an AttributeError
            # (as for example "arg" might be of type ast.Name, which doesn't have a value)
            # Rather than handle all of the possible types, we just catch the error
            raise ValueError("Function parameters not constant or otherwise invalid")
        return fnName, args, kwargs

    def __init__(self, conditions):
        """The condition can be any of these methods which evaulate to a boolean:

            * startswith
            * endswith
            * isalnum
            * isalpha
            * isdigit
            * islower
            * isspace
            * istitle
            * isupper
            * find

        (Find strictly doesn't return a boolean, but it is remapped to one)

        It should be written just like if you were calling the python call yourself.
        For example::

          Filename=startswith('/lhcb')
          Filename=istitle()

        """
        super().__init__(conditions)
        self._fnName, self._fnArgs, self._fnKwargs = self._parseFn(conditions)
        if not self._fnName in self.SUPPORTED_METHODS:
            raise ValueError(f"Function {self._fnName} not supported by this plugin")

    def eval(self, **kwargs):
        """evaluate the parameters. The lfn argument is mandatory"""

        lfn = kwargs.get("lfn")

        if not lfn:
            return False

        try:
            method = getattr(lfn, self._fnName)
            ret = method(*self._fnArgs, **self._fnKwargs)
            # Special case of 'find' which returns -1 if the pattern does not exist
            if self._fnName == "find":
                ret = ret >= 0
            return ret
        except Exception:
            return False
