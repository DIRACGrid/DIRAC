import pytest
from DIRAC.Core.Utilities.ReturnValues import convertToReturnValue
from DIRAC.Core.Security.DiracX import addRPCStub, executeRPCStub, FutureClient


class BadClass:
    """This class does not inherit from FutureClient
    So we should not be able to execute its stub"""

    @addRPCStub
    @convertToReturnValue
    def sum(self, *args, **kwargs):
        """Just sum whatever is given as param"""
        return sum(args + tuple(kwargs.values()))


class Fake(FutureClient):
    @addRPCStub
    @convertToReturnValue
    def sum(self, *args, **kwargs):
        """Just sum whatever is given as param"""
        return sum(args + tuple(kwargs.values()))


def test_rpcStub():
    b = BadClass()
    res = b.sum(1, 2, 3)
    assert res["OK"]
    assert "rpcStub" in res
    stub = res["rpcStub"]
    # Cannot execute this stub as it does not come
    # from a FutureClient
    with pytest.raises(TypeError):
        executeRPCStub(stub)

    def test_sum(f, *args, **kwargs):
        """Test that the original result is the same as the stub"""

        res = f.sum(*args, **kwargs)
        stub = res["rpcStub"]
        replay_res = executeRPCStub(stub)

        assert res["OK"] == replay_res["OK"]
        assert res.get("Value") == replay_res.get("Value")
        assert res.get("Message") == replay_res.get("Message")

    # Test some success cases

    f = Fake()
    test_sum(f, 1, 2, 3)
    test_sum(f, a=3, b=4)
    test_sum(f, 1, 2, a=3, b=4)

    # Test error case

    test_sum(f, "a", None)
