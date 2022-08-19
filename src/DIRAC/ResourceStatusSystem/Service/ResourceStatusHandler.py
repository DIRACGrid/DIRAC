""" ResourceStatusHandler

  Module that allows users to access the ResourceStatusDB remotely.

"""
from DIRAC import gLogger, S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler, getServiceOption
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader


def loadResourceStatusComponent(moduleName, className, parentLogger=None):
    """
    Create an object of a given database component.

    :param moduleName: module name to be loaded
    :param className: class name
    :param parentLogger: the parentLogger to use in the DB
    :return: object instance wrapped in a standard Dirac return object.
    """

    objectLoader = ObjectLoader()
    componentModule = f"ResourceStatusSystem.DB.{moduleName}"
    result = objectLoader.loadObject(componentModule, className)
    if not result["OK"]:
        gLogger.error("Failed to load RSS component", "{}: {}".format(moduleName, result["Message"]))
        return result
    componentClass = result["Value"]
    component = componentClass(parentLogger=parentLogger)
    return S_OK(component)


class ResourceStatusHandlerMixin:
    """
    The ResourceStatusHandler exposes the DB front-end functions through a XML-RPC
    server, functionalities inherited from
    :class:`DIRAC.Core.DISET.RequestHandler.RequestHandler`

    According to the ResourceStatusDB philosophy, only functions of the type:
    - insert
    - update
    - select
    - delete

    are exposed. If you need anything more complicated, either look for it on the
    :class:`ResourceStatusClient`, or code it yourself. This way the DB and the
    Service are kept clean and tidied.
    """

    @classmethod
    def initializeHandler(cls, serviceInfoDict):
        """
        Handler initialization, where we:
          dynamically load ResourceStatus database plugin module, as advised by the config,
          (assumes that the module name and a class name are the same)
          set the ResourceManagementDB as global db.

          :param serviceInfoDict: service info dictionary
          :return: standard Dirac return object

        """

        defaultOption, defaultClass = "ResourceStatusDB", "ResourceStatusDB"
        configValue = getServiceOption(serviceInfoDict, defaultOption, defaultClass)
        result = loadResourceStatusComponent(configValue, configValue, parentLogger=cls.log)

        if not result["OK"]:
            return result

        cls.db = result["Value"]

        return S_OK()

    def __logResult(self, methodName, result):
        """
        Method that writes to log error messages
        """

        if not result["OK"]:
            self.log.error("{}{}".format(methodName, result["Message"]))

    types_insert = [[str, dict], dict]

    def export_insert(self, table, params):
        """
        This method is a bridge to access :class:`ResourceStatusDB` remotely. It
        does not add neither processing nor validation. If you need to know more
        about this method, you must keep reading on the database documentation.

        :Parameters:
          **table** - `string` or `dict`
            should contain the table from which querying
            if it's a `dict` the query comes from a client prior to v6r18

          **params** - `dict`
            arguments for the mysql query. Currently it is being used only for column selection.
            For example: meta = { 'columns' : [ 'Name' ] } will return only the 'Name' column.

        :return: S_OK() || S_ERROR()
        """

        self.log.info(f"insert: {table} {params}")
        res = self.db.insert(table, params)
        self.__logResult("insert", res)

        return res

    types_select = [[str, dict], dict]

    def export_select(self, table, params):
        """
        This method is a bridge to access :class:`ResourceStatusDB` remotely. It
        does not add neither processing nor validation. If you need to know more
        about this method, you must keep reading on the database documentation.

        :Parameters:
          **table** - `string` or `dict`
            should contain the table from which querying
            if it's a `dict` the query comes from a client prior to v6r18

          **params** - `dict`
            arguments for the mysql query. Currently it is being used only for column selection.
            For example: meta = { 'columns' : [ 'Name' ] } will return only the 'Name' column.

        :return: S_OK() || S_ERROR()
        """

        self.log.info(f"select: {table} {params}")
        res = self.db.select(table, params)
        self.__logResult("select", res)

        return res

    types_delete = [[str, dict], dict]

    def export_delete(self, table, params):
        """
    This method is a bridge to access :class:`ResourceStatusDB` remotely.\
    It does not add neither processing nor validation. If you need to know more \
    about this method, you must keep reading on the database documentation.

    :Parameters:
      **table** - `string` or `dict`
        should contain the table from which querying
        if it's a `dict` the query comes from a client prior to v6r18

      **params** - `dict`
        arguments for the mysql query. Currently it is being used only for column selection.
        For example: meta = { 'columns' : [ 'Name' ] } will return only the 'Name' column.


    :return: S_OK() || S_ERROR()
    """

        self.log.info(f"delete: {table} {params}")
        res = self.db.delete(table, params)
        self.__logResult("delete", res)

        return res

    types_addOrModify = [[str, dict], dict]

    def export_addOrModify(self, table, params):
        """
    This method is a bridge to access :class:`ResourceStatusDB` remotely.\
    It does not add neither processing nor validation. If you need to know more \
    about this method, you must keep reading on the database documentation.

    :Parameters:
      **table** - `string` or `dict`
        should contain the table from which querying
        if it's a `dict` the query comes from a client prior to v6r18

      **params** - `dict`
        arguments for the mysql query. Currently it is being used only for column selection.
        For example: meta = { 'columns' : [ 'Name' ] } will return only the 'Name' column.


    :return: S_OK() || S_ERROR()
    """

        self.log.info(f"addOrModify: {table} {params}")
        res = self.db.addOrModify(table, params)
        self.__logResult("addOrModify", res)

        return res

    types_addIfNotThere = [[str, dict], dict]

    def export_addIfNotThere(self, table, params):
        """
    This method is a bridge to access :class:`ResourceStatusDB` remotely.\
    It does not add neither processing nor validation. If you need to know more \
    about this method, you must keep reading on the database documentation.

    :Parameters:
      **table** - `string` or `dict`
        should contain the table from which querying
        if it's a `dict` the query comes from a client prior to v6r18

      **params** - `dict`
        arguments for the mysql query. Currently it is being used only for column selection.
        For example: meta = { 'columns' : [ 'Name' ] } will return only the 'Name' column.


    :return: S_OK() || S_ERROR()
    """

        self.log.info(f"addIfNotThere: {table} {params}")
        res = self.db.addIfNotThere(table, params)
        self.__logResult("addIfNotThere", res)

        return res


class ResourceStatusHandler(ResourceStatusHandlerMixin, RequestHandler):
    pass
