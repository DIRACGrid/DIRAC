import pytest

from DIRACCommon.Core.Utilities.StateMachine import State, StateMachine


class TestState:
    """Tests for the State class"""

    def test_state_basic(self):
        state = State(100)
        assert state.level == 100
        assert state.stateMap == []
        assert state.default is None

    def test_state_with_transitions(self):
        state = State(0, ["State1", "State2"], defState="State1")
        assert state.level == 0
        assert state.stateMap == ["State1", "State2"]
        assert state.default == "State1"

    def test_transition_rule_in_map(self):
        state = State(0, ["State1", "State2"], defState="State1")
        assert state.transitionRule("State2") == "State2"

    def test_transition_rule_not_in_map_with_default(self):
        state = State(0, ["State1", "State2"], defState="State1")
        assert state.transitionRule("UnknownState") == "State1"

    def test_transition_rule_not_in_map_without_default(self):
        state = State(0, ["State1", "State2"])
        assert state.transitionRule("UnknownState") == "UnknownState"


class TestStateMachine:
    """Tests for the StateMachine class"""

    def test_get_level_of_state(self):
        sm = StateMachine()
        assert sm.getLevelOfState("Nirvana") == 100
        assert sm.getLevelOfState("UnknownState") == -1

    def test_get_states(self):
        sm = StateMachine()
        assert sm.getStates() == ["Nirvana"]

    def test_set_state_none_candidate(self):
        sm = StateMachine(state="Nirvana")
        result = sm.setState(None)
        assert result["OK"] is True
        assert result["Value"] is None

    def test_set_state_same_state(self):
        sm = StateMachine(state="Nirvana")
        result = sm.setState("Nirvana")
        assert result["OK"] is True
        assert result["Value"] == "Nirvana"

    def test_set_state_invalid_candidate(self):
        sm = StateMachine(state="Nirvana")
        result = sm.setState("InvalidState")
        assert result["OK"] is False

    def test_set_state_from_none_to_valid(self):
        """Test transitioning from None state to a valid state"""
        sm = StateMachine(state=None)
        result = sm.setState("Nirvana")
        assert result["OK"] is True
        assert result["Value"] == "Nirvana"

    def test_set_state_to_none_then_to_valid(self):
        """Test setting state to None and then to a valid state - reproduces KeyError bug"""
        sm = StateMachine(state="Nirvana")
        # First transition to None
        result = sm.setState(None)
        assert result["OK"] is True
        assert result["Value"] is None
        # Then transition to a valid state - this should not raise KeyError
        result = sm.setState("Nirvana")
        assert result["OK"] is True
        assert result["Value"] == "Nirvana"

    def test_get_next_state_with_none_current(self):
        sm = StateMachine(state=None)
        result = sm.getNextState("Nirvana")
        assert result["OK"] is True
        assert result["Value"] == "Nirvana"

    def test_get_next_state_invalid(self):
        sm = StateMachine(state="Nirvana")
        result = sm.getNextState("InvalidState")
        assert result["OK"] is False
