"""
DummySyncPlugin
"""


class DummySyncPlugin:
    """Dummy Synchronization plugin that does nothing.
    It is used to document what are the requirements for a real Sync plugin.

    Such plugins are meant to validate user's information about to be added to the CS,
    or to complete it with various sources.
    They are called by the
    :py:meth:`DIRAC.ConfigurationSystem.Client.VOMS2CSSynchronizer.VOMS2CSSynchronizer.syncCSWithVOMS`
    """

    def __init__(self):
        """The constructor does not receive any argument.
        Note that the plugin is instanciated only once, so this is
        a good place to do global initialization.
        """
        pass

    def verifyAndUpdateUserInfo(self, username, userDict):
        """
          This method is expected to validate the user's data passed
          as parameter, but is also allowed to extend them.

          In case the validation was not to pass, this method must raise
          ``ValueError``. The user would then not be added to the CS.

        :param username: DIRAC name of the user to be added
        :param userDict: user information collected by the VOMS2CSAgent. Typical keys include
                         ``DN``,``CA``, ``Email``, ``Groups``
        :returns: None
        :raise ValueError: in case user information do not pass validation

        """
        raise NotImplementedError("Must be implemented by the plugin")
