"""
BaseBody serves as a base class for all the BodyPlugin, as well as documentation
reference for that object.
"""

from DIRAC.Core.Utilities.JEncode import JSerializable


class BaseBody(JSerializable):
    """
    Body plugins are meant to add evolved logic into the process of
    turning a TransformationSystem Task into an RMS Request.

    All your BodyPlugins should inherit from this class. Note that ``BaseBody``
    inherit from :py:class:`DIRAC.Core.Utilities.JEncode.JSerializable`, so you
    should follow the guidelines from this class too.
    """

    def taskToRequest(self, taskID, task, transID):
        """
        This is the only method needed by your plugin. Its role
        is to turn the task into a Request.
        Note that this method is called for one task at the time.

        :param taskID: id for the task
        :param task: dict describing the task
        :param transID: id of the transformation

        :return: A request object
        """
        raise NotImplementedError()
