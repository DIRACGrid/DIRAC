import time


def compare_results(monkeypatch, test_func):
    """Compare the results from DIRAC and DiracX based services for a reentrant function."""
    compare_results2(monkeypatch, test_func, test_func)


def compare_results2(monkeypatch, test_func1, test_func2):
    """Compare the results from DIRAC and DiracX based services for two functions which should behave identically."""
    # Get the result from the diracx-based handler
    start = time.monotonic()
    with monkeypatch.context() as m:
        m.setattr("DIRAC.Core.Tornado.Client.ClientSelector.useLegacyAdapter", lambda *_: True)
        try:
            future_result = test_func1()
        except Exception as e:
            future_result = e
        else:
            assert "rpcStub" not in future_result, "rpcStub should never be present when using DiracX!"
    diracx_duration = time.monotonic() - start

    # Get the result from the DIRAC-based handler
    start = time.monotonic()
    with monkeypatch.context() as m:
        m.setattr("DIRAC.Core.Tornado.Client.ClientSelector.useLegacyAdapter", lambda *_: False)
        old_result = test_func2()
        assert "rpcStub" in old_result, "rpcStub should always be present when using legacy DIRAC!"
    legacy_duration = time.monotonic() - start

    # We don't care about the rpcStub or Errno
    old_result.pop("rpcStub")
    old_result.pop("Errno", None)

    if not old_result["OK"]:
        assert not future_result["OK"], "FutureClient should have failed too!"
    elif "Value" in future_result:
        # Ensure the results match exactly
        assert old_result == future_result
    else:
        # See the "stripValueIfOK" decorator for explanation
        assert old_result["OK"] == future_result["OK"]
        # assert isinstance(old_result["Value"], int)

    # if 3 * legacy_duration < diracx_duration:
    #     print(f"Legacy DIRAC took {legacy_duration:.3f}s, FutureClient took {diracx_duration:.3f}s")
    #     assert False, "FutureClient should be faster than legacy DIRAC!"
