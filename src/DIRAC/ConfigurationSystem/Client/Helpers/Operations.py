""" This helper looks in the /Operations section of the CS and in /SenSettings section to get VO/setup settings.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from os.path import join

from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.ConfigurationSystem.private.ConfigurationClient import ConfigurationClient


class Operations(ConfigurationClient):
    """Operations class

    The /Operations CFG section is maintained in a cache by an Operations object
    """

    def __init__(self, vo=None, group=None, setup=None):
        """Determination of VO/setup and generation a list of relevant directories

        :param str vo: VO name
        :param str group: group name
        :param str setup: setup name
        """
        super(Operations, self).__init__(vo=getVOForGroup(group or "") or vo, setup=setup, rootPath="/Operations")
