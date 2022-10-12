"""
  This module defines the state machine for the Productions
"""
from DIRAC.Core.Utilities.StateMachine import State, StateMachine


class ProductionsStateMachine(StateMachine):
    """Production Management System implementation of the state machine"""

    def __init__(self, state):
        """c'tor
        Defines the state machine transactions
        """
        super().__init__(state)

        # States transitions
        self.states = {
            "Cleaned": State(5),  # final state
            # The Cleaning State should be added later. This State implies that there is an agent checking if
            # all the associated transformations are cleaned and which automatically updates the State to
            # Cleaned. For the moment the transition is directly from Stopped to Cleaned.
            # 'Cleaning'   : State( 3, ['Cleaned'] ),
            "Completed": State(4, ["Cleaned"], defState="Completed"),
            "Stopped": State(3, ["Active", "Flush", "Cleaned"], defState="Stopped"),
            "Flush": State(2, ["Active", "Cleaned"], defState="Flush"),
            "Active": State(1, ["Flush", "Stopped", "Cleaned"], defState="Active"),
            # initial state
            "New": State(0, ["Active", "Cleaned"], defState="New"),
        }
