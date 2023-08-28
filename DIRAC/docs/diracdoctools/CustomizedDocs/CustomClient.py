"""Additional docstring for the Client module."""
import textwrap

MODULE = "DIRAC.Core.Base.Client"


class CustomClient:  # pylint: disable=too-few-public-methods
    """Add the initialize function to Core.Base.Client docs"""

    def __init__(self):
        """Create the customized documentation including the DIRAC.initialize function."""
        self.module = MODULE
        self.replace = False
        self.doc_string = ""
        # Fill the docstring addition with what we want to add
        self.doc_string += textwrap.dedent(
            """
            Initialization
            ==============

            Before clients can be used DIRAC's internal state must be configured.
            For ``dirac-`` commands this is handled by :py:class:`DIRAC.Core.Base.Script.Script`.
            For all other purposes ``DIRAC.initialize`` should be used:

            .. autofunction:: DIRAC.initialize
            """
        )


CUSTOMIZED_DOCSTRINGS[MODULE] = CustomClient()  # pylint: disable=undefined-variable
