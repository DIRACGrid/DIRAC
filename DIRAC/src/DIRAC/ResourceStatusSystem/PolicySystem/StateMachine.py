""" StateMachine

  This module contains the RSS implementation of StateMachine, using its own states map.

"""
from DIRAC.Core.Utilities.StateMachine import State, StateMachine


class RSSMachine(StateMachine):
    """
    RSS implementation of the State Machine. It defines six states, which ordered
    by level conform the following list ( higher level first ): Unknown, Active,
    Degraded, Probing, Banned, Error.

    The StateMachine allows any transition except if the current state is Banned,
    which will force any transition to any state different of Error, Banned and
    Probing to Probing.

      examples:
        >>> rsm0 = RSSMachine( None )
        >>> rsm1 = RSSMachine( 'Unknown' )

    :param state: name of the current state of the StateMachine
    :type state: None or str

    """

    def __init__(self, state):
        """
        Constructor.

        """

        super().__init__(state)

        # Defines state map.
        self.states = {
            "Unknown": State(5),
            "Active": State(4),
            "Degraded": State(3),
            "Probing": State(2),
            "Banned": State(1, ["Error", "Banned", "Probing"], defState="Probing"),
            "Error": State(0),
        }

    def orderPolicyResults(self, policyResults):
        """
        Method built specifically to interact with the policy results obtained on the
        PDP module. It sorts the input based on the level of their statuses, the lower
        the level state, the leftmost position in the list. Beware, if any of the statuses
        is not know to the StateMachine, it will be ordered first, as its level will be
        -1 !.

        examples:
          >>> rsm0.orderPolicyResults( [ { 'Status' : 'Active', 'A' : 'A' },
                                         { 'Status' : 'Banned', 'B' : 'B' } ] )
              [ { 'Status' : 'Banned', 'B' : 'B' }, { 'Status' : 'Active', 'A' : 'A' } ]
          >>> rsm0.orderPolicyResults( [ { 'Status' : 'Active', 'A' : 'A' },
                                         { 'Status' : 'Rubbish', 'R' : 'R' } ] )
              [ { 'Status' : 'Rubbish', 'R' : 'R' }, { 'Status' : 'Active', 'A' : 'A' } ]


        :param policyResults: list of dictionaries to be ordered. The dictionary can have any key as
            far as the key `Status` is present.
        :type policyResults: python:list

        :result: list( dict ), which is ordered
        """

        # We really do not need to return, as the list is mutable
        policyResults.sort(key=self.getLevelOfPolicyState)

    def getLevelOfPolicyState(self, policyResult):
        """
        Returns the level of the state associated with the policy, -1 if something
        goes wrong. It is mostly used while sorting policies with method `orderPolicyResults`.

        examples:
          >>> rsm0.getLevelOfPolicyState( { 'Status' : 'Active', 'A' : 'A' } )
              5
          >>> rsm0.getLevelOfPolicyState( { 'Status' : 'Rubbish', 'R' : 'R' } )
              -1

        :param dict policyResult: dictionary that must have the `Status` key.
        :return: int || -1 ( if policyResult[ 'Status' ] is not known by the StateMachine )
        """

        return self.getLevelOfState(policyResult["Status"])
