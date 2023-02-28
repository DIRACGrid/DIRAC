"""
This Service provides functionality to access and modify the
InstalledComponentsDB database
"""
from DIRAC import S_OK, S_ERROR, gLogger

from DIRAC.FrameworkSystem.DB.InstalledComponentsDB import (
    InstalledComponentsDB,
    Component,
    Host,
    InstalledComponent,
    HostLogging,
)
from DIRAC.Core.DISET.RequestHandler import RequestHandler


class ComponentMonitoringHandlerMixin:
    @classmethod
    def initializeHandler(cls, serviceInfo):
        """
        Handler class initialization
        """

        ComponentMonitoringHandler.doCommit = True

        try:
            ComponentMonitoringHandler.db = InstalledComponentsDB()
        except Exception:
            gLogger.exception()
            return S_ERROR("Could not connect to the database")

        return S_OK("Initialization went well")

    def __joinInstallationMatch(self, installationFields, componentFields, hostFields):
        matchFields = installationFields
        for key in componentFields:
            matchFields["Component." + key] = componentFields[key]
        for key in hostFields:
            matchFields["Host." + key] = hostFields[key]

        return S_OK(matchFields)

    types_addComponent = [dict]

    def export_addComponent(self, component):
        """
        Creates a new Component object on the database
        component argument should be a dictionary with the Component fields and
        its values
        """

        return ComponentMonitoringHandler.db.addComponent(component)

    types_componentExists = [dict]

    def export_componentExists(self, matchFields):
        """
        Returns whether components matching the given criteria exist
        matchFields argument should be a dictionary with the fields to match
        matchFields accepts fields of the form <Field.bigger> and <Field.smaller>
        to filter using > and < relationships.
        """

        return ComponentMonitoringHandler.db.exists(Component, matchFields)

    types_getComponents = [dict, bool, bool]

    def export_getComponents(self, matchFields, includeInstallations, includeHosts):
        """
        Returns a list of all the Components in the database
        matchFields argument should be a dictionary with the fields to match or
        empty to get all the instances
        matchFields also accepts fields of the form <Field.bigger> and
        <Field.smaller> to filter using > and < relationships
        includeInstallations indicates whether data about the installations in
        which the components takes part is to be retrieved
        includeHosts (only if includeInstallations is set to True) indicates
        whether data about the host in which there are instances of this component
        is to be retrieved
        """

        return ComponentMonitoringHandler.db.getComponents(matchFields, includeInstallations, includeHosts)

    types_updateComponents = [dict, dict]

    def export_updateComponents(self, matchFields, updates):
        """
        Updates Components objects on the database
        matchFields argument should be a dictionary with the fields to match
        (instances matching the fields will be updated) or empty to update all
        the instances
        matchFields also accepts fields of the form <Field.bigger> and
        <Field.smaller> to filter using > and < relationships updates argument
        updates should be a dictionary with the Component fields and their new
        updated values
        """

        return ComponentMonitoringHandler.db.updateComponents(matchFields, updates)

    types_removeComponents = [dict]

    def export_removeComponents(self, matchFields):
        """
        Removes from the database components that match the given fields
        matchFields argument should be a dictionary with the fields to match or
        empty to remove all the instances
        matchFields also accepts fields of the form <Field.bigger> and
        <Field.smaller> to filter using > and < relationships
        """

        return ComponentMonitoringHandler.db.removeComponents(matchFields)

    types_addHost = [dict]

    def export_addHost(self, host):
        """
        Creates a new Host object on the database
        host argument should be a dictionary with the Host fields and its values
        """

        return ComponentMonitoringHandler.db.addHost(host)

    types_hostExists = [dict]

    def export_hostExists(self, matchFields):
        """
        Returns whether hosts matching the given criteria exist
        matchFields argument should be a dictionary with the fields to match
        matchFields also accepts fields of the form <Field.bigger> and
        <Field.smaller> to filter using > and < relationships
        """

        return ComponentMonitoringHandler.db.exists(Host, matchFields)

    types_getHosts = [dict, bool, bool]

    def export_getHosts(self, matchFields, includeInstallations, includeComponents):
        """
        Returns a list of all the Hosts in the database
        matchFields argument should be a dictionary with the fields to match or
        empty to get all the instances
        matchFields also accepts fields of the form <Field.bigger> and
        <Field.smaller> to filter using > and < relationships
        includeInstallations indicates whether data about the installations in
        which the host takes part is to be retrieved
        includeComponents (only if includeInstallations is set to True) indicates
        whether data about the components installed into this host is to
        be retrieved
        """

        return ComponentMonitoringHandler.db.getHosts(matchFields, includeInstallations, includeComponents)

    types_updateHosts = [dict, dict]

    def export_updateHosts(self, matchFields, updates):
        """
        Updates Hosts objects on the database
        matchFields argument should be a dictionary with the fields to
        match (instances matching the fields will be updated) or empty to update
        all the instances
        matchFields also accepts fields of the form <Field.bigger> and
        <Field.smaller> to filter using > and < relationships updates argument
        should be a dictionary with the Host fields and their new updated values
        updates argument should be a dictionary with the Host fields and
        their new updated values
        """

        return ComponentMonitoringHandler.db.updateHosts(matchFields, updates)

    types_removeHosts = [dict]

    def export_removeHosts(self, matchFields):
        """
        Removes from the database hosts that match the given fields
        matchFields argument should be a dictionary with the fields to match or
        empty to remove all the instances matchFields also accepts fields of the
        form <Field.bigger> and <Field.smaller> to filter
        using > and < relationships
        """

        return ComponentMonitoringHandler.db.removeHosts(matchFields)

    types_addInstallation = [dict, dict, dict, bool]

    def export_addInstallation(self, installation, componentDict, hostDict, forceCreate):
        """
        Creates a new InstalledComponent object on the database
        installation argument should be a dictionary with the InstalledComponent
        fields and its values
        componentDict argument should be a dictionary with the Component fields
        and its values
        hostDict argument should be a dictionary with the Host fields and
        its values
        forceCreate indicates whether a new Component and/or Host should be
        created if the given ones do not exist
        """

        return ComponentMonitoringHandler.db.addInstalledComponent(installation, componentDict, hostDict, forceCreate)

    types_installationExists = [dict, dict, dict]

    def export_installationExists(self, installationFields, componentFields, hostFields):
        """
        Returns whether installations matching the given criteria exist
        installationFields argument should be a dictionary with the fields to
        match for the installation
        componentFields argument should be a dictionary with the fields to match
        for the component installed
        hostFields argument should be a dictionary with the fields to match for
        the host where the installation is made
        """

        matchFields = self.__joinInstallationMatch(installationFields, componentFields, hostFields)["Value"]

        return ComponentMonitoringHandler.db.exists(InstalledComponent, matchFields)

    types_getInstallations = [dict, dict, dict, bool]

    def export_getInstallations(self, installationFields, componentFields, hostFields, installationsInfo):
        """
        Returns a list of installations matching the given criteria
        installationFields argument should be a dictionary with the fields to
        match for the installation
        componentFields argument should be a dictionary with the fields to match
        for the component installed
        hostFields argument should be a dictionary with the fields to match for
        the host where the installation is made
        installationsInfo indicates whether information about the components and
        host taking part in the installation is to be provided
        """

        matchFields = self.__joinInstallationMatch(installationFields, componentFields, hostFields)["Value"]

        return ComponentMonitoringHandler.db.getInstalledComponents(matchFields, installationsInfo)

    types_updateInstallations = [dict, dict, dict, dict]

    def export_updateInstallations(self, installationFields, componentFields, hostFields, updates):
        """
        Updates installations matching the given criteria
        installationFields argument should be a dictionary with the fields to
        match for the installation
        componentFields argument should be a dictionary with the fields to match
        for the component installed or empty to update regardless of component
        hostFields argument should be a dictionary with the fields to match for
        the host where the installation is made or empty to update
        regardless of host
        updates argument should be a dictionary with the Installation fields and
        their new updated values
        """

        matchFields = self.__joinInstallationMatch(installationFields, componentFields, hostFields)["Value"]

        return ComponentMonitoringHandler.db.updateInstalledComponents(matchFields, updates)

    types_removeInstallations = [dict, dict, dict]

    def export_removeInstallations(self, installationFields, componentFields, hostFields):
        """
        Removes installations matching the given criteria
        installationFields argument should be a dictionary with the fields to
        match for the installation
        componentFields argument should be a dictionary with the fields to match
        for the component installed
        hostFields argument should be a dictionary with the fields to match for
        the host where the installation is made
        """

        matchFields = self.__joinInstallationMatch(installationFields, componentFields, hostFields)["Value"]

        return ComponentMonitoringHandler.db.removeInstalledComponents(matchFields)

    types_updateLog = [str, dict]

    def export_updateLog(self, host, fields):
        """
        Updates the log entry for a given host in the database with the given fields
        host is the name of the machine to which the logging information belongs
        fields is a dictionary where the fields contain the logging information to be stored in the database
        """
        result = ComponentMonitoringHandler.db.exists(HostLogging, {"hostName": host})
        if not result["OK"]:
            return result

        if result["Value"]:
            result = ComponentMonitoringHandler.db.updateLogs({"hostName": host}, fields)
        else:
            fields["hostName"] = host
            result = ComponentMonitoringHandler.db.addLog(fields)

        if not result["OK"]:
            return result

        return S_OK("Logs updated correctly")

    types_getLog = [str]

    def export_getLog(self, host):
        """
        Retrieves the logging information currently stored for the given host
        """
        result = ComponentMonitoringHandler.db.exists(HostLogging, {"hostName": host})
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR(f"Host {host} does not exist")

        return ComponentMonitoringHandler.db.getLogs({"hostName": host})

    types_getLogs = []

    def export_getLogs(self):
        """
        Retrieves the logging information currently stored for all hosts
        """
        return ComponentMonitoringHandler.db.getLogs()

    types_removeLogs = [dict]

    def export_removeLogs(self, fields):
        """
        Deletes all the matching logging information
        fields is a dictionary containing the values for the fields
        such that any matching entries in the database should be deleted
        """
        result = ComponentMonitoringHandler.db.exists(HostLogging, fields)
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR("Host does not exist")

        return ComponentMonitoringHandler.db.removeLogs(fields)


class ComponentMonitoringHandler(ComponentMonitoringHandlerMixin, RequestHandler):
    pass
