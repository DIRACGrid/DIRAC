"""
  Defines the plugin to perform evaluation on the user in the proxy
"""
import re


from DIRAC.Resources.Catalog.ConditionPlugins.FCConditionBasePlugin import FCConditionBasePlugin
from DIRAC.Core.DISET.ThreadConfig import ThreadConfig
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers import Registry


class ProxyPlugin(FCConditionBasePlugin):
    """
    This plugin is to be used when filtering based on the user contained in the proxy
    """

    def __init__(self, conditions):
        """This plugin allows to perform tests on the proxy.
        Supported conditions are:

          * username.in(<comma separated list of names>): the user should be in the defined list
          * username.not_in(<comma separated list of names>): the user should *not* be in the defined list
          * group.in(<comma separated list of names>): the group should be in the defined list
          * group.not_in(<comma separated list of names>): the group should *not* be in the defined list
          * property.has(<property name>): the proxy should have the given property
          * property.has_not(<property name>): the proxy should *not* have the given property
          * voms.has(<voms role>) (see below): if the given VOMS role is associated to the proxy
          * voms.has_not(<voms role>)  (see below): if the given VOMS role is *not* associated to the proxy

        Because it is not possible to use the '=' sign, the VOMS role has to be declared using the
        symbol '->'.
        For example::

          "voms.has(/lhcb/Role->production)" will look for "/lhcb/Role=production" in the VOMS list contained
          in the proxy info.


        If the conditions does not follow the form 'attribute.predicate(value)', an exception
        is thrown, and will lead to all the expression be evaluated to False

        If there is no proxy, all conditions are evaluated to False

        """
        super().__init__(conditions)

        # the conditions have the form
        # attribute.predicate(value)
        condition_pattern = r"^(\w+)\.(\w+)\((.+)\)$"
        regex = re.compile(condition_pattern)
        match = regex.search(conditions)
        self.attr, self.predicate, self.value = match.groups()

        # To cover the case where the names would be surrounded by (single) quotes
        self.value = self.value.replace("'", "").replace('"', "").replace(" ", "")

        self._checkCondition()
        self.proxyInfo = getProxyInfo().get("Value")

        # We may not have a proxy, check the thread local
        if not self.proxyInfo:
            tc = ThreadConfig()
            userDN = tc.getDN()
            userGroup = tc.getGroup()
            if userDN and userGroup:
                userName = Registry.getUsernameForDN(userDN).get("Value")
                if userName:
                    self.proxyInfo = {"username": userName, "group": userGroup}

    def _checkCondition(self):
        """Checks that the actual condition makes sense
        if not, raises a RuntimeError exception
        """

        excp = RuntimeError("Incorrect condition format %s" % self.conditions)

        if self.attr in ["username", "group"]:
            if self.predicate not in ["in", "not_in"]:
                raise excp
        elif self.attr in ["property", "voms"]:
            if self.predicate not in ["has", "has_not"]:
                raise excp
        else:
            raise excp

    def eval(self, **kwargs):
        """evaluate the parameters."""

        if not self.proxyInfo:
            return False

        valueToLookFor = None
        listToLookInto = []

        if self.attr in ["username", "group"]:
            valueToLookFor = self.proxyInfo.get(self.attr)
            listToLookInto = self.value.split(",")
        elif self.attr == "property":
            valueToLookFor = self.value
            listToLookInto = self.proxyInfo.get("groupProperties", [])
        elif self.attr == "voms":
            valueToLookFor = self.value.replace("->", "=")
            listToLookInto = self.proxyInfo.get("VOMS", [])

        if "not" in self.predicate:
            return valueToLookFor not in listToLookInto
        else:
            return valueToLookFor in listToLookInto
