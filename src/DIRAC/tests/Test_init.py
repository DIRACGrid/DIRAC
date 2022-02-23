import pytest

import DIRAC


@pytest.mark.parametrize(
    "path, expected",
    [
        ("/opt/dirac/versions/v7.2.1-1622561290/Linux-x86_64/", "/opt/dirac"),
        ("/opt/dirac/versions/v10.2.1-1622561290/Linux-x86_64/", "/opt/dirac"),
        ("/opt/dirac/versions/v10.2.0a14.dev11+ga5b9ec512-1622561290/Linux-x86_64/", "/opt/dirac"),
        ("/opt/dirac/versions/v10.200.999-1622561290/Linux-x86_64/", "/opt/dirac"),
        ("/opt/dirac/versions/v487.200.999-1622561290/Linux-x86_64/", "/opt/dirac"),
        ("/opt/dirac/versions/v10.2.1a1-1622561290/Linux-x86_64/", "/opt/dirac"),
        ("/opt/dirac/versions/v10.2.1a12-1622561290/Linux-x86_64/", "/opt/dirac"),
        ("/opt/dirac/versions/v10.2.1b10-1622561290/Linux-x86_64/", "/opt/dirac"),
        ("/opt/dirac/versions/v10.2.1rc2-1622561290/Linux-x86_64/", "/opt/dirac"),
        ("/opt/dirac/versions/v10.2.1-1622561290/Linux-ppc64le/", "/opt/dirac"),
        ("/opt/dirac/versions/v10.2.1-1622561290/Linux-aarch64/", "/opt/dirac"),
        ("/opt/dirac/versions/v10.2.1-1622561290/Darwin-x86_64/", "/opt/dirac"),
        ("/opt/dirac/versions/v10.2.1-1622561290/Darwin-arm64/", "/opt/dirac"),
        ("/cvmfs/lhcb.cern.ch/lhcbdirac/versions/v10.2.1-1622561290/Linux-x86_64/", "/cvmfs/lhcb.cern.ch/lhcbdirac"),
        ("/cvmfs/lhcb.cern.ch/lhcbdirac/versions/v10.2.1a1-1622561290/Linux-x86_64/", "/cvmfs/lhcb.cern.ch/lhcbdirac"),
        ("/diracos/", "/diracos"),
        ("/", "/"),
        ("/opt/dirac/", "/opt/dirac"),
        ("/opt/versions/", "/opt/versions"),
        ("/opt/versions/1234", "/opt/versions/1234"),
        ("/opt/versions/diracos", "/opt/versions/diracos"),
    ],
)
def test_computeRootPath(path, expected):
    assert DIRAC._computeRootPath(path) == expected
    # Trailing / shouldn't affect the rootPath
    assert DIRAC._computeRootPath(path + "//") == expected
    if path != "/":
        assert DIRAC._computeRootPath(path.rstrip("/")) == expected
