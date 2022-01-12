""" VOBOXAvailabilityCommand module
"""
# FIXME: NOT Usable ATM
# missing doNew, doCache, doMaster

from urllib import parse

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.Client import Client
from DIRAC.ResourceStatusSystem.Command.Command import Command


class VOBOXAvailabilityCommand(Command):
    """
    Given an url pointing to a service on a vobox, use DIRAC ping against it.
    """

    def doCommand(self):
        """
        The Command pings a service on a vobox, it needs a service URL to ping it.

        :returns: a dict with the following:

          .. code-block:: python

            {
              'serviceUpTime' : <serviceUpTime>,
              'machineUpTime' : <machineUpTime>,
              'site'          : <site>,
              'system'        : <system>,
              'service'       : <service>
            }

        """

        # INPUT PARAMETERS

        if "serviceURL" not in self.args:
            return self.returnERROR(S_ERROR('"serviceURL" not found in self.args'))
        serviceURL = self.args["serviceURL"]

        ##

        parsed = parse.urlparse(serviceURL)
        site = parsed[1].split(":")[0]

        try:
            system, service = parsed[2].strip("/").split("/")
        except ValueError:
            return self.returnERROR(S_ERROR('"%s" seems to be a malformed url' % serviceURL))

        pinger = Client(url=serviceURL)
        resPing = pinger.ping()

        if not resPing["OK"]:
            return self.returnERROR(resPing)

        serviceUpTime = resPing["Value"].get("service uptime", 0)
        machineUpTime = resPing["Value"].get("host uptime", 0)

        result = {
            "site": site,
            "system": system,
            "service": service,
            "serviceUpTime": serviceUpTime,
            "machineUpTime": machineUpTime,
        }

        return S_OK(result)

        # FIXME: how do we get the values !!
