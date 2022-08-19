"""Test the JobInfo"""
import unittest
import sys
from io import StringIO

from unittest.mock import MagicMock as Mock

from parameterized import parameterized, param

from DIRAC import S_OK, S_ERROR, gLogger
import DIRAC

from DIRAC.TransformationSystem.Utilities.JobInfo import TaskInfoException, JobInfo
import DIRAC.Interfaces.API.Dirac

gLogger.setLevel("DEBUG")
# pylint: disable=W0212, E1101


class TestJI(unittest.TestCase):
    """Test the JobInfo Module"""

    def setUp(self):
        self.jbi = JobInfo(jobID=123, status="Failed", tID=1234, tType="MCReconstruction")
        self.diracAPI = Mock(name="dilcMock", spec=DIRAC.Interfaces.API.Dirac.Dirac)
        self.jobMon = Mock(
            name="jobMonMock", spec=DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient.JobMonitoringClient
        )
        self.jobMon.getInputData = Mock(return_value=S_OK([]))
        self.jobMon.getJobAttribute = Mock(return_value=S_OK("0"))
        self.jobMon.getJobParameter = Mock(return_value=S_OK({}))
        self.diracAPI.getJobJDL = Mock()

        self.jdl2 = {
            "LogTargetPath": "/ilc/prod/clic/500gev/yyveyx_o/ILD/REC/00006326/LOG/00006326_015.tar",
            "Executable": "dirac-jobexec",
            "TaskID": 15,
            "SoftwareDistModule": "ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation",
            "JobName": "00006326_00000015",
            "Priority": 1,
            "Platform": "x86_64-slc5-gcc43-opt",
            "JobRequirements": {
                "OwnerDN": "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sailer/CN=683529/CN=Andre Sailer",
                "VirtualOrganization": "ilc",
                "Setup": "ILC-Production",
                "CPUTime": 300000,
                "OwnerGroup": "ilc_prod",
                "Platforms": [
                    "x86_64-slc6-gcc44-opt",
                    "x86_64-slc5-gcc43-opt",
                    "slc5_amd64_gcc43",
                    "Linux_x86_64_glibc-2.12",
                    "Linux_x86_64_glibc-2.5",
                ],
                "UserPriority": 1,
                "Sites": [
                    "LCG.LAPP.fr",
                    "LCG.UKI-SOUTHGRID-RALPP.uk",
                ],
                "BannedSites": "LCG.KEK.jp",
                "JobTypes": "MCReconstruction_Overlay",
            },
            "Arguments": "jobDescription.xml -o LogLevel=verbose",
            "SoftwarePackages": [
                "overlayinput.1",
                "marlin.v0111Prod",
            ],
            "DebugLFNs": "",
            "Status": "Created",
            "InputDataModule": "DIRAC.WorkloadManagementSystem.Client.InputDataResolution",
            "BannedSites": "LCG.KEK.jp",
            "LogLevel": "verbose",
            "InputSandbox": [
                "jobDescription.xml",
                "SB:ProductionSandboxSE2|/SandBox/i/ilc_prod/5d3/92f/5d392f5266a796018ab6774ef84cbd31.tar.bz2",
            ],
            "OwnerName": "sailer",
            "StdOutput": "std.out",
            "JobType": "MCReconstruction_Overlay",
            "GridEnv": "/cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example",
            "TransformationID": 6326,
            "DIRACSetup": "ILC-Production",
            "StdError": "std.err",
            "IS_PROD": "True",
            "OwnerDN": "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sailer/CN=683529/CN=Andre Sailer",
            "JobGroup": 0o0006326,
            "OutputSandbox": [
                "std.err",
                "std.out",
            ],
            "JobID": 15756436,
            "VirtualOrganization": "ilc",
            "ProductionOutputData": [
                "/ilc/prod/clic/500gev/yyveyx_o/ILD/REC/00006326/000/yyveyx_o_rec_6326_15.slcio",
                "/ilc/prod/clic/500gev/yyveyx_o/ILD/DST/00006326/000/yyveyx_o_dst_6326_15.slcio",
            ],
            "Site": "ANY",
            "OwnerGroup": "ilc_prod",
            "Owner": "sailer",
            "LogFilePath": "/ilc/prod/clic/500gev/yyveyx_o/ILD/REC/00006326/LOG/000",
            "InputData": "/ilc/prod/clic/500gev/yyveyx_o/ILD/SIM/00006325/000/yyveyx_o_sim_6325_17.slcio",
        }

        self.jdlBrokenContent = {
            "LogTargetPath": "/ilc/prod/clic/500gev/yyveyx_o/ILD/REC/00006326/LOG/00006326_015.tar",
            "Executable": "dirac-jobexec",
            "TaskID": "muahahaha",
            "SoftwareDistModule": "ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation",
            "JobName": "00006326_00000015",
            "Priority": 1,
            "Platform": "x86_64-slc5-gcc43-opt",
            "JobRequirements": {
                "OwnerDN": "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sailer/CN=683529/CN=Andre Sailer",
                "VirtualOrganization": "ilc",
                "Setup": "ILC-Production",
                "CPUTime": 300000,
                "OwnerGroup": "ilc_prod",
                "Platforms": [
                    "x86_64-slc6-gcc44-opt",
                    "x86_64-slc5-gcc43-opt",
                    "slc5_amd64_gcc43",
                    "Linux_x86_64_glibc-2.12",
                    "Linux_x86_64_glibc-2.5",
                ],
                "UserPriority": 1,
                "Sites": [
                    "LCG.LAPP.fr",
                    "LCG.UKI-SOUTHGRID-RALPP.uk",
                ],
                "BannedSites": "LCG.KEK.jp",
                "JobTypes": "MCReconstruction_Overlay",
            },
            "Arguments": "jobDescription.xml -o LogLevel=verbose",
            "SoftwarePackages": [
                "overlayinput.1",
                "marlin.v0111Prod",
            ],
            "DebugLFNs": "",
            "Status": "Created",
            "InputDataModule": "DIRAC.WorkloadManagementSystem.Client.InputDataResolution",
            "BannedSites": "LCG.KEK.jp",
            "LogLevel": "verbose",
            "InputSandbox": [
                "jobDescription.xml",
                "SB:ProductionSandboxSE2|/SandBox/i/ilc_prod/5d3/92f/5d392f5266a796018ab6774ef84cbd31.tar.bz2",
            ],
            "OwnerName": "sailer",
            "StdOutput": "std.out",
            "JobType": "MCReconstruction_Overlay",
            "GridEnv": "/cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example",
            "TransformationID": 6326,
            "DIRACSetup": "ILC-Production",
            "StdError": "std.err",
            "IS_PROD": "True",
            "OwnerDN": "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sailer/CN=683529/CN=Andre Sailer",
            "JobGroup": 0o0006326,
            "OutputSandbox": [
                "std.err",
                "std.out",
            ],
            "JobID": 15756436,
            "VirtualOrganization": "ilc",
            "ProductionOutputData": [
                "/ilc/prod/clic/500gev/yyveyx_o/ILD/REC/00006326/000/yyveyx_o_rec_6326_15.slcio",
                "/ilc/prod/clic/500gev/yyveyx_o/ILD/DST/00006326/000/yyveyx_o_dst_6326_15.slcio",
            ],
            "Site": "ANY",
            "OwnerGroup": "ilc_prod",
            "Owner": "sailer",
            "LogFilePath": "/ilc/prod/clic/500gev/yyveyx_o/ILD/REC/00006326/LOG/000",
            "InputData": "/ilc/prod/clic/500gev/yyveyx_o/ILD/SIM/00006325/000/yyveyx_o_sim_6325_17.slcio",
        }

        # jdl with single outputdata,
        self.jdl1 = {
            "LogTargetPath": "/ilc/prod/clic/3tev/e1e1_o/SID/SIM/00006301/LOG/00006301_10256.tar",
            "Executable": "dirac-jobexec",
            "TaskID": 10256,
            "SoftwareDistModule": "ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation",
            "JobName": "00006301_00010256",
            "Priority": 1,
            "Platform": "x86_64-slc5-gcc43-opt",
            "JobRequirements": {
                "OwnerDN": "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sailer/CN=683529/CN=Andre Sailer",
                "VirtualOrganization": "ilc",
                "Setup": "ILC-Production",
                "CPUTime": 300000,
                "OwnerGroup": "ilc_prod",
                "Platforms": [
                    "x86_64-slc6-gcc44-opt",
                    "x86_64-slc5-gcc43-opt",
                    "slc5_amd64_gcc43",
                    "Linux_x86_64_glibc-2.12",
                    "Linux_x86_64_glibc-2.5",
                ],
                "UserPriority": 1,
                "Sites": [
                    "LCG.LAPP.fr",
                    "LCG.UKI-SOUTHGRID-RALPP.uk",
                ],
                "BannedSites": [
                    "OSG.MIT.us",
                    "OSG.SPRACE.br",
                ],
                "JobTypes": "MCSimulation",
            },
            "Arguments": "jobDescription.xml -o LogLevel=verbose",
            "SoftwarePackages": "slic.v2r9p8",
            "DebugLFNs": "",
            "Status": "Created",
            "InputDataModule": "DIRAC.WorkloadManagementSystem.Client.InputDataResolution",
            "BannedSites": [
                "OSG.MIT.us",
                "OSG.SPRACE.br",
            ],
            "LogLevel": "verbose",
            "InputSandbox": [
                "jobDescription.xml",
                "SB:ProductionSandboxSE2|/SandBox/i/ilc_prod/042/d64/042d64cb0fe73720cbd114a73506c582.tar.bz2",
            ],
            "OwnerName": "sailer",
            "StdOutput": "std.out",
            "JobType": "MCSimulation",
            "GridEnv": "/cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example",
            "TransformationID": 6301,
            "DIRACSetup": "ILC-Production",
            "StdError": "std.err",
            "IS_PROD": "True",
            "OwnerDN": "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sailer/CN=683529/CN=Andre Sailer",
            "JobGroup": "00006301",
            "OutputSandbox": [
                "std.err",
                "std.out",
            ],
            "JobID": 15756456,
            "VirtualOrganization": "ilc",
            "ProductionOutputData": "/ilc/prod/clic/3tev/e1e1_o/SID/SIM/00006301/010/e1e1_o_sim_6301_10256.slcio",
            "Site": "ANY",
            "OwnerGroup": "ilc_prod",
            "Owner": "sailer",
            "LogFilePath": "/ilc/prod/clic/3tev/e1e1_o/SID/SIM/00006301/LOG/010",
            "InputData": "/ilc/prod/clic/3tev/e1e1_o/gen/00006300/004/e1e1_o_gen_6300_4077.stdhep",
        }

        self.jdlNoInput = {
            "LogTargetPath": "/ilc/prod/clic/1.4tev/ea_qqqqnu/gen/00006498/LOG/00006498_1307.tar",
            "Executable": "dirac-jobexec",
            "TaskID": 1307,
            "SoftwareDistModule": "ILCDIRAC.Core.Utilities.CombinedSoftwareInstallation",
            "JobName": "00006498_00001307",
            "Priority": 1,
            "Platform": "x86_64-slc5-gcc43-opt",
            "JobRequirements": {
                "OwnerDN": "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sailer/CN=683529/CN=Andre Sailer",
                "VirtualOrganization": "ilc",
                "Setup": "ILC-Production",
                "CPUTime": 300000,
                "OwnerGroup": "ilc_prod",
                "Platforms": [
                    "x86_64-slc6-gcc44-opt",
                    "x86_64-slc5-gcc43-opt",
                    "slc5_amd64_gcc43",
                    "Linux_x86_64_glibc-2.12",
                    "Linux_x86_64_glibc-2.5",
                ],
                "UserPriority": 1,
                "BannedSites": "LCG.KEK.jp",
                "JobTypes": "MCGeneration",
            },
            "Arguments": "jobDescription.xml -o LogLevel=verbose",
            "SoftwarePackages": "whizard.SM_V57",
            "DebugLFNs": "",
            "Status": "Created",
            "InputDataModule": "DIRAC.WorkloadManagementSystem.Client.InputDataResolution",
            "BannedSites": "LCG.KEK.jp",
            "LogLevel": "verbose",
            "InputSandbox": [
                "jobDescription.xml",
                "SB:ProductionSandboxSE2|/SandBox/i/ilc_prod/b2a/d98/b2ad98c3e240361a4253c4bb277be478.tar.bz2",
            ],
            "OwnerName": "sailer",
            "StdOutput": "std.out",
            "JobType": "MCGeneration",
            "GridEnv": "/cvmfs/grid.cern.ch/emi-ui-3.7.3-1_sl6v2/etc/profile.d/setup-emi3-ui-example",
            "TransformationID": 6498,
            "DIRACSetup": "ILC-Production",
            "StdError": "std.err",
            "IS_PROD": "True",
            "OwnerDN": "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=sailer/CN=683529/CN=Andre Sailer",
            "JobGroup": "00006498",
            "OutputSandbox": [
                "std.err",
                "std.out",
            ],
            "JobID": 15762268,
            "VirtualOrganization": "ilc",
            "ProductionOutputData": "/ilc/prod/clic/1.4tev/ea_qqqqnu/gen/00006498/001/ea_qqqqnu_gen_6498_1307.stdhep",
            "Site": "ANY",
            "OwnerGroup": "ilc_prod",
            "Owner": "sailer",
            "LogFilePath": "/ilc/prod/clic/1.4tev/ea_qqqqnu/gen/00006498/LOG/001",
            "InputData": "",
        }

    def tearDown(self):
        pass

    def test_Init(self):
        """Transformation.Utilities.JobInfo init ...................................................."""
        assert self.jbi.outputFiles == []
        self.assertFalse(self.jbi.pendingRequest)

    def test_allFilesExist(self):
        """Transformation.Utilities.JobInfo.allFilesExist............................................"""
        self.jbi.outputFileStatus = ["Exists", "Exists"]
        self.assertTrue(self.jbi.allFilesExist())
        self.jbi.outputFileStatus = ["Exists", "Missing"]
        self.assertFalse(self.jbi.allFilesExist())
        self.jbi.outputFileStatus = ["Missing", "Exists"]
        self.assertFalse(self.jbi.allFilesExist())
        self.jbi.outputFileStatus = ["Missing", "Missing"]
        self.assertFalse(self.jbi.allFilesExist())
        self.jbi.outputFileStatus = []
        self.assertFalse(self.jbi.allFilesExist())

    def test_allFilesMissing(self):
        """Transformation.Utilities.JobInfo.allFilesMissing.........................................."""
        self.jbi.outputFileStatus = ["Exists", "Exists"]
        self.assertFalse(self.jbi.allFilesMissing())
        self.jbi.outputFileStatus = ["Exists", "Missing"]
        self.assertFalse(self.jbi.allFilesMissing())
        self.jbi.outputFileStatus = ["Missing", "Exists"]
        self.assertFalse(self.jbi.allFilesMissing())
        self.jbi.outputFileStatus = ["Missing", "Missing"]
        self.assertTrue(self.jbi.allFilesMissing())
        self.jbi.outputFileStatus = []
        self.assertFalse(self.jbi.allFilesMissing())

    @parameterized.expand(
        [
            ("someFilesMissing", "outputFileStatus", ["Exists", "Exists"], False),
            ("someFilesMissing", "outputFileStatus", ["Exists", "Missing"], True),
            ("someFilesMissing", "outputFileStatus", ["Missing", "Exists"], True),
            ("someFilesMissing", "outputFileStatus", ["Missing", "Missing"], False),
            ("someFilesMissing", "outputFileStatus", [], False),
            ("allInputFilesExist", "inputFileStatus", ["Exists", "Exists"], True),
            ("allInputFilesExist", "inputFileStatus", ["Exists", "Missing"], False),
            ("allInputFilesExist", "inputFileStatus", ["Missing", "Missing"], False),
            ("allInputFilesExist", "inputFileStatus", [], False),
            ("allInputFilesMissing", "inputFileStatus", ["Exists", "Exists"], False),
            ("allInputFilesMissing", "inputFileStatus", ["Exists", "Missing"], False),
            ("allInputFilesMissing", "inputFileStatus", ["Missing", "Missing"], True),
            ("allInputFilesMissing", "inputFileStatus", [], False),
            ("someInputFilesMissing", "inputFileStatus", ["Exists", "Exists"], False),
            ("someInputFilesMissing", "inputFileStatus", ["Exists", "Missing"], True),
            ("someInputFilesMissing", "inputFileStatus", ["Missing", "Exists"], True),
            ("someInputFilesMissing", "inputFileStatus", ["Missing", "Missing"], False),
            ("someInputFilesMissing", "inputFileStatus", [], False),
            ("allFilesProcessed", "transFileStatus", ["Processed", "Processed"], True),
            ("allFilesProcessed", "transFileStatus", ["Processed", "Assigned"], False),
            ("allFilesProcessed", "transFileStatus", ["Assigned", "Assigned"], False),
            ("allFilesProcessed", "transFileStatus", ["Deleted", "Deleted"], False),
            ("allFilesProcessed", "transFileStatus", ["Unused", "Unused"], False),
            ("allFilesProcessed", "transFileStatus", [], False),
            ("allFilesAssigned", "transFileStatus", ["Processed", "Processed"], True),
            ("allFilesAssigned", "transFileStatus", ["Processed", "Assigned"], True),
            ("allFilesAssigned", "transFileStatus", ["Assigned", "Assigned"], True),
            ("allFilesAssigned", "transFileStatus", ["Assigned", "Unused"], False),
            ("allFilesAssigned", "transFileStatus", ["Deleted", "Deleted"], False),
            ("allFilesAssigned", "transFileStatus", ["Unused", "Unused"], False),
            ("allFilesAssigned", "transFileStatus", [], False),
            ("checkErrorCount", "errorCounts", [0, 9], False),
            ("checkErrorCount", "errorCounts", [0, 10], False),
            ("checkErrorCount", "errorCounts", [0, 11], True),
            ("checkErrorCount", "errorCounts", [0, 12], True),
            ("allTransFilesDeleted", "transFileStatus", ["Deleted", "Deleted"], True),
            ("allTransFilesDeleted", "transFileStatus", ["Deleted", "Assigned"], False),
            ("allTransFilesDeleted", "transFileStatus", ["Assigned", "Deleted"], False),
            ("allTransFilesDeleted", "transFileStatus", ["Assigned", "Assigned"], False),
        ]
    )
    def test_fileChecker(self, func, attr, value, expected):
        setattr(self.jbi, attr, value)
        gLogger.notice(f"{getattr(self.jbi, func)()}, {func}, {attr}, {value}, {expected}")
        assert expected == getattr(self.jbi, func)()

    def test_getJDL(self):
        """Transformation.Utilities.JobInfo.getJDL..................................................."""

        self.diracAPI.getJobJDL.return_value = S_OK(self.jdl1)
        jdlList = self.jbi._JobInfo__getJDL(self.diracAPI)
        self.assertIsInstance(jdlList, dict)

        self.diracAPI.getJobJDL.return_value = S_ERROR("no mon")
        with self.assertRaises(RuntimeError) as contextManagedException:
            jdlList = self.jbi._JobInfo__getJDL(self.diracAPI)
        self.assertIn("Failed to get jobJDL", str(contextManagedException.exception))

    def test_getTaskInfo_1(self):
        # task is only one
        wit = ["MCReconstruction"]
        self.jbi.taskID = 1234
        self.jbi.inputFiles = ["lfn"]
        tasksDict = {1234: [dict(FileID=123456, LFN="lfn", Status="Assigned", ErrorCount=7)]}
        lfnTaskDict = {}
        self.jbi.getTaskInfo(tasksDict, lfnTaskDict, wit)
        self.assertEqual(self.jbi.transFileStatus, ["Assigned"])
        self.assertEqual(self.jbi.otherTasks, [])

    def test_getTaskInfo_2(self):
        # there are other tasks
        wit = ["MCReconstruction"]
        self.jbi.taskID = 1234
        self.jbi.inputFiles = ["lfn"]
        tasksDict = {12: [dict(FileID=123456, LFN="lfn", Status="Processed", ErrorCount=7)]}
        lfnTaskDict = {"lfn": 12}
        self.jbi.getTaskInfo(tasksDict, lfnTaskDict, wit)
        self.assertEqual(self.jbi.transFileStatus, ["Processed"])
        self.assertEqual(self.jbi.otherTasks, [12])

    def test_getTaskInfo_3(self):
        # raise
        wit = ["MCReconstruction"]
        self.jbi.taskID = 1234
        self.jbi.inputFiles = ["otherLFN"]
        tasksDict = {1234: [dict(FileID=123456, LFN="lfn", Status="Processed", ErrorCount=23)]}
        lfnTaskDict = {}
        with self.assertRaisesRegex(TaskInfoException, "InputFiles do not agree"):
            self.jbi.getTaskInfo(tasksDict, lfnTaskDict, wit)

    # def test_getTaskInfo_4(self):
    #   # raise keyError
    #   wit = ['MCReconstruction']
    #   self.jbi.taskID = 1235
    #   self.jbi.inputFiles = []
    #   tasksDict = {1234: dict(FileID=123456, LFN="lfn", Status="Processed")}
    #   lfnTaskDict = {}
    #   with self.assertRaisesRegex(KeyError, ""):
    #     self.jbi.getTaskInfo(tasksDict, lfnTaskDict, wit)

    def test_getTaskInfo_5(self):
        # raise inputFile
        wit = ["MCReconstruction"]
        self.jbi.taskID = 1235
        self.jbi.inputFiles = []
        tasksDict = {1234: dict(FileID=123456, LFN="lfn", Status="Processed")}
        lfnTaskDict = {}
        with self.assertRaisesRegex(TaskInfoException, "InputFiles is empty"):
            self.jbi.getTaskInfo(tasksDict, lfnTaskDict, wit)

    def test_getJobInformation(self):
        """Transformation.Utilities.JobInfo.getJobInformation........................................"""
        self.diracAPI.getJobJDL.return_value = S_OK(self.jdl1)
        self.jbi.getJobInformation(self.diracAPI, self.jobMon)
        self.assertEqual(
            self.jbi.outputFiles, ["/ilc/prod/clic/3tev/e1e1_o/SID/SIM/00006301/010/e1e1_o_sim_6301_10256.slcio"]
        )
        self.assertEqual(10256, self.jbi.taskID)
        self.assertEqual(
            self.jbi.inputFiles, ["/ilc/prod/clic/3tev/e1e1_o/gen/00006300/004/e1e1_o_gen_6300_4077.stdhep"]
        )

        # empty jdl
        self.setUp()
        self.diracAPI.getJobJDL.return_value = S_OK({})
        self.jbi.getJobInformation(self.diracAPI, self.jobMon)
        self.assertEqual(self.jbi.outputFiles, [])
        self.assertIsNone(self.jbi.taskID)
        self.assertEqual(self.jbi.inputFiles, [])

    def test_getOutputFiles(self):
        """Transformation.Utilities.JobInfo.getOutputFiles..........................................."""
        # singleLFN
        self.diracAPI.getJobJDL.return_value = S_OK(self.jdl1)
        jdlList = self.jbi._JobInfo__getJDL(self.diracAPI)
        self.jbi._JobInfo__getOutputFiles(jdlList)
        self.assertEqual(
            self.jbi.outputFiles, ["/ilc/prod/clic/3tev/e1e1_o/SID/SIM/00006301/010/e1e1_o_sim_6301_10256.slcio"]
        )

        # two LFNs
        self.diracAPI.getJobJDL.return_value = S_OK(self.jdl2)
        jdlList = self.jbi._JobInfo__getJDL(self.diracAPI)
        self.jbi._JobInfo__getOutputFiles(jdlList)
        self.assertEqual(
            self.jbi.outputFiles,
            [
                "/ilc/prod/clic/500gev/yyveyx_o/ILD/REC/00006326/000/yyveyx_o_rec_6326_15.slcio",
                "/ilc/prod/clic/500gev/yyveyx_o/ILD/DST/00006326/000/yyveyx_o_dst_6326_15.slcio",
            ],
        )

    def test_getTaskID(self):
        """Transformation.Utilities.JobInfo.getTaskID................................................"""
        # singleLFN
        self.diracAPI.getJobJDL.return_value = S_OK(self.jdl1)
        jdlList = self.jbi._JobInfo__getJDL(self.diracAPI)
        self.jbi._JobInfo__getTaskID(jdlList)
        self.assertEqual(10256, self.jbi.taskID)

        # broken jdl
        out = StringIO()
        sys.stdout = out
        self.diracAPI.getJobJDL.return_value = S_OK(self.jdlBrokenContent)
        jdlList = self.jbi._JobInfo__getJDL(self.diracAPI)
        with self.assertRaises(ValueError):
            self.jbi._JobInfo__getTaskID(jdlList)

    def test_getInputFile(self):
        """Test the extraction of the inputFile from the JDL parameters."""
        # singleLFN
        self.jbi._JobInfo__getInputFile({"InputData": "/single/lfn2"})
        self.assertEqual(self.jbi.inputFiles, ["/single/lfn2"])

        # list with singleLFN
        self.jbi._JobInfo__getInputFile({"InputData": ["/single/lfn1"]})
        self.assertEqual(self.jbi.inputFiles, ["/single/lfn1"])

        # list with two LFN
        self.jbi._JobInfo__getInputFile({"InputData": ["/lfn1", "/lfn2"]})
        self.assertEqual(self.jbi.inputFiles, ["/lfn1", "/lfn2"])

    def test_checkFileExistence(self):
        """Transformation.Utilities.JobInfo.checkFileExistance......................................."""
        # input and output files
        repStatus = {"inputFile1": True, "inputFile2": False, "outputFile1": False, "outputFile2": True}
        self.jbi.inputFiles = ["inputFile1", "inputFile2", "inputFile3"]
        self.jbi.outputFiles = ["outputFile1", "outputFile2", "unknownFile"]
        self.jbi.checkFileExistence(repStatus)
        self.assertTrue(self.jbi.inputFilesExist[0])
        self.assertFalse(self.jbi.inputFilesExist[1])
        self.assertFalse(self.jbi.inputFilesExist[2])
        self.assertEqual(self.jbi.inputFileStatus, ["Exists", "Missing", "Unknown"])
        self.assertEqual(self.jbi.outputFileStatus, ["Missing", "Exists", "Unknown"])

        # just output files
        self.setUp()
        repStatus = {"inputFile": True, "outputFile1": False, "outputFile2": True}
        self.jbi.inputFiles = []
        self.jbi.outputFiles = ["outputFile1", "outputFile2", "unknownFile"]
        self.jbi.checkFileExistence(repStatus)
        self.assertEqual(self.jbi.outputFileStatus, ["Missing", "Exists", "Unknown"])

    @parameterized.expand(
        [
            param(
                ["123: Failed MCReconstruction Transformation: 1234 -- 5678 ", "inputFile (True, Assigned, Errors 0"],
                [],
            ),
            param(
                ["123: Failed MCReconstruction Transformation: 1234 -- 5678  (Last task [7777])"], [], otherTasks=[7777]
            ),
            param([], ["MCReconstruction Transformation"], trID=0, taID=0),
            param([], ["(Last task"], otherTasks=[]),
            param(
                ["PENDING REQUEST IGNORE THIS JOB"],
                [],
                pendingRequest=True,
            ),
            param(
                ["No Pending Requests"],
                [],
                pendingRequest=False,
            ),
        ]
    )
    def test__str__(self, asserts, assertNots, trID=1234, taID=5678, otherTasks=False, pendingRequest=False):
        jbi = JobInfo(jobID=123, status="Failed", tID=trID, tType="MCReconstruction")
        jbi.pendingRequest = pendingRequest
        jbi.otherTasks = otherTasks
        gLogger.notice("otherTasks: ", jbi.otherTasks)
        jbi.taskID = taID
        jbi.inputFiles = ["inputFile"]
        jbi.inputFilesExist = [True]
        jbi.transFileStatus = ["Assigned"]
        jbi.outputFiles = ["outputFile"]
        jbi.errorCounts = [0]
        info = str(jbi)
        for assertStr in asserts:
            self.assertIn(assertStr, info)
        for assertStr in assertNots:
            self.assertNotIn(assertStr, info)

    def test_TaskInfoException(self):
        """Transformation.Utilities.JobInfo.TaskInfoException........................................"""
        tie = TaskInfoException("notTasked")
        self.assertIsInstance(tie, Exception)
        self.assertIn("notTasked", str(tie))


if __name__ == "__main__":
    SUITE = unittest.defaultTestLoader.loadTestsFromTestCase(TestJI)
    TESTRESULT = unittest.TextTestRunner(verbosity=3).run(SUITE)
