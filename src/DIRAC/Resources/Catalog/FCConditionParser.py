"""
   Contains the mechanism to evaluate whether to use or not a catalog
"""
from pyparsing import infixNotation, opAssoc, Word, printables, Literal, Suppress

from DIRAC import S_OK, gLogger
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations


class FCConditionParser:
    """This objects allows to evaluate conditions on whether or not
    a given operation should be evaluated on a given catalog
    for a given lfn (be glad so many things are given to you !).

    The conditions are expressed as boolean logic, where the basic bloc has
    the form "pluginName=whateverThatWillBePassedToThePlugin".
    The basic blocs will be evaluated by the respective plugins, and the result can
    be combined using the standard boolean operators::

      * `!` for not
      * `&` for and
      * `|` for or
      * `[ ]` for prioritizing the operations

    All these characters, as well as the '=' symbol cannot be used in any expression to be
    evaluated by a plugin.

    The rule to evaluate can either be given at calling time, or can be retrieved
    from the CS depending on the context (see doc of __call__ and __getConditionFromCS)


    Example of rules are::

      * Filename=startswith('/lhcb') & Proxy=voms.has(/lhcb/Role->production)
      * [Filename=startswith('/lhcb') & !Filename=find('/user/')] | Proxy=group.in(lhcb_mc, lhcb_data)
    """

    # Some characters are reserved for the grammar
    __forbidenChars = ("[", "]", "!", "&", "|", "=")
    __allowedChars = "".join(set(printables) - set(__forbidenChars)) + " "

    # Defines the basic shape of a rule : pluginName=whateverThatWillBePassedToThePlugin
    __pluginOperand = Word(__allowedChars) + Literal("=") + Word(__allowedChars)

    # define classes to be built at parse time, as each matching
    # expression type is parsed

    # Binary operator base class
    class _BoolBinOp:
        """Abstract object to represent a binary operator"""

        reprsymbol = None  # Sign to represent the boolean operation

        # This is the boolean operation to apply
        # Could be None, but it should be callable
        @staticmethod
        def evalop(_x):
            return None

        def __init__(self, token):
            """
            :param token: the token matching a binary operator
                          it is a list with only one element which itself is a list
                          [ [ Arg1, Operator, Arg2] ]
                          The arguments themselves can be of any type, but they need to
                          provide an "eval" method that takes `kwargs` as input,
                          and return a boolean
            """

            # Keep the two arguments
            self.args = token[0][0::2]

        def __str__(self):
            """String representation"""

            sep = " %s " % self.reprsymbol
            return "(" + sep.join(map(str, self.args)) + ")"

        def eval(self, **kwargs):
            """Perform the evaluation of the boolean logic
            by applying the operator between the two arguments

            :param kwargs: whatever information is given to plugin (typically lfn)
            """

            return self.evalop(arg.eval(**kwargs) for arg in self.args)

        __repr__ = __str__

    class _BoolAnd(_BoolBinOp):
        """Represents the 'and' operator"""

        reprsymbol = "&"
        evalop = all

    class _BoolOr(_BoolBinOp):
        """Represents the 'or' operator"""

        reprsymbol = "|"
        evalop = any

    class _BoolNot:
        """Represents the "not" unitary operator"""

        def __init__(self, t):
            """
            :param t: the token matching a unitary operator
                      it is a list with only one element which itself is a list
                      [ [ !, Arg1] ]
                      The argument itself can be of any type, but it needs to
                      provide an "eval" method that takes `kwargs` as input,
                      and return a boolean
            """

            # We just keep the argument
            self.arg = t[0][1]

        def eval(self, **kwargs):
            """Perform the evaluation of the boolean logic
            by returning the negation of the evaluation of the argument

            :param kwargs: whatever information is given to plugin (typically lfn)
            """
            return not self.arg.eval(**kwargs)

        def __str__(self):
            return "!" + str(self.arg)

        __repr__ = __str__

    # We can combine the pluginOperand with boolean expression,
    # and prioritized by squared bracket
    __boolExpr = infixNotation(
        __pluginOperand,
        [
            ("!", 1, opAssoc.RIGHT, _BoolNot),
            ("&", 2, opAssoc.LEFT, _BoolAnd),
            ("|", 2, opAssoc.LEFT, _BoolOr),
        ],
        lpar=Suppress("["),
        rpar=Suppress("]"),
    )

    # Wrapper that will call the plugin
    class PluginOperand:
        """This class is a wrapper for a plugin
        and it's condition
        It is instantiated by pyparsing every time
        it encounters "plugin=condition"

        """

        def __init__(self, tokens):
            """
            :param tokens: [ pluginName, =, conditions ]
                   the pluginName is automatically prepended with 'Plugin'


            """

            self.pluginName = "%sPlugin" % tokens[0].strip(" ")
            self.conditions = tokens[2].strip(" ")

            # Load the plugin, and give it the condition
            objLoader = ObjectLoader()
            _class = objLoader.loadObject("Resources.Catalog.ConditionPlugins.%s" % self.pluginName)

            if not _class["OK"]:
                raise Exception(_class["Message"])

            self._pluginInst = _class["Value"](self.conditions)

        def eval(self, **kwargs):
            """Forward the evaluation call to the plugin

            :param kwargs: contains all the information given to the plugin namely the lfns
            :return: True or False
            """

            return self._pluginInst.eval(**kwargs)

        def __str__(self):
            return self.pluginName

        __repr__ = __str__

    def __init__(self, vo=None, ro_methods=None):
        """
        :param vo: name of the VO
        """

        # Whenever we parse text matching the __pluginOperand grammar, create a PluginOperand object
        self.__pluginOperand.setParseAction(lambda tokens: self.PluginOperand(tokens))

        self.opHelper = Operations(vo=vo)

        self.ro_methods = ro_methods if ro_methods else []

        self.log = gLogger.getSubLogger(self.__class__.__name__)

    def __evaluateCondition(self, conditionString, **kwargs):
        """Evaluate a condition against attributes, typically lfn.
        CAUTION: lfns are here given one by one

        """

        self.log.debug(f"Testing {conditionString} against {kwargs}")

        # Parse all the condition and evaluate it
        # res is a tuple whose first and only element is either
        # one of the bool operator defined above, or a PluginOperand
        res = self.__boolExpr.parseString(conditionString)
        res = res[0].eval(**kwargs)

        self.log.debug("Evaluated to %s" % res)

        return res

    def __getConditionFromCS(self, catalogName, operationName):
        """Retrieves the appropriate condition from the CS
        The base path is in Operation/[Setup/Default]/DataManagement/FCConditions/[CatalogName]
        If there are no condition defined for the method, we check the global READ/WRITE condition.
        If this does not exist either, we check the global ALL condition.
        If none is defined, we return None

        :param str catalogName: the catalog we want to work on
        :param str operationName: the operation we want to perform
                                  The operationName must be in the read or write method from FileCatalog

        :returns: a condition string or None


        """
        basePath = "Services/Catalogs/%s/Conditions/" % catalogName
        pathList = [
            basePath + "%s" % operationName,
            basePath + "%s" % ("READ" if operationName in self.ro_methods else "WRITE"),
            basePath + "ALL",
        ]

        for path in pathList:
            condVal = self.opHelper.getValue(path)
            if condVal:
                return condVal

    def __call__(self, catalogName, operationName, lfns, condition=None, **kwargs):
        """
        Makes a boolean evaluation of a condition, for a given catalog,
        a given operation, and a list of lfns. Extra parameters might be given,
        and will be forwarded to each plugin.
        If the 'condition' attribute is not specified (general case), it is fetched from the CS
        (see __getConditionFromCS)

        If there are no condition at all, return True for everything.
        A programming error in the plugins will lead to the evaluation being False

        .. Note::

          if the CS can't be contacted, the conditions will be evaluated to None
          (courtesy of the Operation helper), so everything will be evaluated to True.
          Ultimately, it does not really matter, since you will not be able to find
          any catalog beforehand if you can't contact the CS...

        :param str catalogName: name of the catalog we want to work on
        :param str operationName: name of the operation we want to perform
                              The operationName must be in the read or write method from FileCatalog
                              if it should be retrieve from the CS
        :param lfns: list/dict of lfns

          .. warning::

             LFNs are expected to have been through the normalizing process, so it
             should not be a string

        :param condition: condition string. If not specified, will be fetched from the CS
        :param kwargs: extra params forwarded to the plugins

        :return: S_OK with a 'Successful' dict {lfn:True/False} where the value is the evaluation of the
                condition against the given lfn key. Failed dict is always empty


        """
        self.log.debug(f"Testing {operationName} on {catalogName} for {len(lfns)} lfns")

        conditionStr = condition if condition is not None else self.__getConditionFromCS(catalogName, operationName)

        self.log.debug("Condition string: %s" % conditionStr)

        evaluatedLfns = {}

        if conditionStr:
            for lfn in lfns:
                try:
                    evaluatedLfns[lfn] = self.__evaluateCondition(conditionStr, lfn=lfn, **kwargs)
                except Exception as e:
                    self.log.exception("Exception while evaluation conditions", lException=e)
                    evaluatedLfns[lfn] = False
        else:
            evaluatedLfns = dict.fromkeys(lfns, True)

        return S_OK({"Successful": evaluatedLfns, "Failed": {}})
