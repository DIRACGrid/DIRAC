# content of conftest.py


def pytest_addoption(parser):
    parser.addoption(
        "--seName",
        action="store",
        nargs=1,
        required=True,
        help="StorageElement name to test",
    )
    parser.addoption(
        "--protocolSection",
        action="append",
        required=False,
        help="Protocol sections of the SE to test",
    )


def pytest_generate_tests(metafunc):
    if metafunc.definition.name == "test_storage_element":
        # Make sure we only test one SE at the time
        seNameList = metafunc.config.getoption("seName")

        metafunc.parametrize("seName", seNameList)

        protocolSections = metafunc.config.getoption("protocolSection")

        if not protocolSections:
            from DIRAC.Resources.Storage.StorageElement import StorageElement

            protocolSections = StorageElement(seNameList[0]).getProtocolSections()["Value"]
            # protocolSections = ["a", "b", "c"]

        metafunc.parametrize("protocolSection", protocolSections)
