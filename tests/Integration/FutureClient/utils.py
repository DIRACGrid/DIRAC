def compare_results(test_func):
    """Compare the results from DIRAC and DiracX based services for a reentrant function."""
    ClientClass = test_func.func.__self__
    assert ClientClass.diracxClient, "FutureClient is not set up!"

    # Get the result from the diracx-based handler
    future_result = test_func()

    # Get the result from the DIRAC-based handler
    diracxClient = ClientClass.diracxClient
    ClientClass.diracxClient = None
    try:
        old_result = test_func()
    finally:
        ClientClass.diracxClient = diracxClient
    # We don't care about the rpcStub
    old_result.pop("rpcStub")

    # Ensure the results match
    assert old_result == future_result
