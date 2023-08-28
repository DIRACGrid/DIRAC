""" Utility for loading plotting types.
    Works both for Accounting and Monitoring.
"""
import re

from DIRAC.Core.Utilities.ObjectLoader import loadObjects

from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType
from DIRAC.MonitoringSystem.Client.Types.BaseType import BaseType

########################################################################


class TypeLoader:
    """
    .. class:: BaseType

    :param dict loaded: it stores the loaded classes
    :param str path: The location of the classes
    :param ~DIRAC.MonitoringSystem.Client.Types.BaseType.BaseType parentCls: it is the parent class
    :param regexp: regular expression...
    """

    ########################################################################
    def __init__(self, plottingFamily="Accounting"):
        """c'tor"""
        self.__loaded = {}
        if plottingFamily == "Accounting":
            self.__path = "AccountingSystem/Client/Types"
            self.__parentCls = BaseAccountingType
        elif plottingFamily == "Monitoring":
            self.__path = "MonitoringSystem/Client/Types"
            self.__parentCls = BaseType
        self.__reFilter = re.compile(r".*[a-z1-9]\.py$")

    ########################################################################
    def getTypes(self):
        """
        It returns all monitoring classes
        """
        if not self.__loaded:
            self.__loaded = loadObjects(self.__path, self.__reFilter, self.__parentCls)
        return self.__loaded
