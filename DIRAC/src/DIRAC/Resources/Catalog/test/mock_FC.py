""" Module that contains a mock object of the FileCatalog(s)
"""
# pylint: disable=invalid-name, line-too-long

import datetime
from unittest.mock import MagicMock

from DIRAC import S_OK

fc_mock = MagicMock()

fc_mock.listDirectory.return_value = {
    "OK": True,
    "Value": {
        "Failed": {},
        "Successful": {
            "/this/is/dir1/": {
                "Datasets": {},
                "Files": {
                    "/this/is/dir1/file1.txt": {
                        "MetaData": {
                            "Checksum": "7149ed85",
                            "ChecksumType": "Adler32",
                            "CreationDate": datetime.datetime(2014, 12, 4, 12, 16, 56),
                            "FileID": 156301805,  # noqa
                            "GID": 2695,  # noqa
                            "GUID": "6A5C6C86-AD7B-E411-9EDB",
                            "Mode": 436,
                            "ModificationDate": datetime.datetime(2014, 12, 4, 12, 16, 56),
                            "Owner": "phicharp",
                            "OwnerGroup": "lhcb_prod",
                            "Size": 206380531,  # noqa
                            "Status": "AprioriGood",
                            "Type": "File",
                            "UID": 19503,  # noqa
                        }
                    },
                    "/this/is/dir1/file2.foo.bar": {
                        "MetaData": {
                            "Checksum": "7149ed86",
                            "ChecksumType": "Adler32",
                            "CreationDate": datetime.datetime(2014, 12, 4, 12, 16, 56),
                            "FileID": 156301805,  # noqa
                            "GID": 2695,  # noqa
                            "GUID": "6A5C6C86-AD7B-E411-9EDB",
                            "Mode": 436,
                            "ModificationDate": datetime.datetime(2014, 12, 4, 12, 16, 56),
                            "Owner": "phicharp",
                            "OwnerGroup": "lhcb_prod",
                            "Size": 206380532,  # noqa
                            "Status": "AprioriGood",
                            "Type": "File",
                            "UID": 19503,  # noqa
                        }
                    },
                },
                "Links": {},
                "SubDirs": {},
            },
            "/this/is/dir2/": {
                "Datasets": {},
                "Files": {},
                "Links": {},
                "SubDirs": {"/this/is/dir2/subdir1/": True, "/this/is/dir2/subdir2/": True},
            },
            "/this/is/dir2/subdir1/": {
                "Datasets": {},
                "Files": {
                    "/this/is/dir2/subdir1/file3.pippo": {
                        "MetaData": {
                            "Checksum": "7149ed86",
                            "ChecksumType": "Adler32",
                            "CreationDate": datetime.datetime(2014, 12, 4, 12, 16, 56),
                            "FileID": 156301805,  # noqa
                            "GID": 2695,  # noqa
                            "GUID": "6A5C6C86-AD7B-E411-9EDB",
                            "Mode": 436,
                            "ModificationDate": datetime.datetime(2014, 12, 4, 12, 16, 56),
                            "Owner": "phicharp",
                            "OwnerGroup": "lhcb_prod",
                            "Size": 206380532,  # noqa
                            "Status": "AprioriGood",
                            "Type": "File",
                            "UID": 19503,  # noqa
                        }
                    }
                },
                "Links": {},
                "SubDirs": {},
            },
            "/this/is/dir2/subdir2/": {"Datasets": {}, "Files": {}, "Links": {}, "SubDirs": {}},
        },
    },
}


fc_mock.getReplicas.return_value = {
    "OK": True,
    "Value": {
        "Failed": {},
        "Successful": {
            "/this/is/dir1/file1.txt": {
                "SE1": "smr://srm.SE1.ch:8443/srm/v2/server?SFN=/this/is/dir1/file1.txt",
                "SE2": "smr://srm.SE2.fr:8443/srm/v2/server?SFN=/this/is/dir1/file1.txt",
            },
            "/this/is/dir1/file2.foo.bar": {
                "SE1": "smr://srm.SE1.ch:8443/srm/v2/server?SFN=/this/is/dir1/file2.foo.bar",
                "SE3": "smr://srm.SE3.es:8443/srm/v2/server?SFN=/this/is/dir1/file2.foo.bar",
            },
        },
    },
}

fc_mock.getFileMetadata.return_value = S_OK(
    {"Successful": {"/a/lfn/1.txt": {"GUID": "AABB11"}, "/a/lfn/2.txt": {"GUID": "AABB22"}}, "Failed": {}}
)
