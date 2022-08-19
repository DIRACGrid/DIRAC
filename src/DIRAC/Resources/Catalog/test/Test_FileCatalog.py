"""
   Testing the FileCatalog logic
"""
import sys
import unittest
from unittest import mock

import DIRAC
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

from DIRAC import S_OK, S_ERROR

current_module = sys.modules[__name__]


class GenericCatalog:
    """Dummy catalog"""

    def __init__(self, name, nb_read, nb_read_no_lfn, nb_write, nb_write_no_lfn):

        self.w_method = []
        self.r_method = []
        self.no_lfn = []
        self.name = name

        self.__generateMethods("read", self.r_method, nb_read, nb_read_no_lfn)
        self.__generateMethods("write", self.w_method, nb_write, nb_write_no_lfn)

    def hasCatalogMethod(self, methName):
        return methName in self.w_method or methName in self.r_method

    def __generateMethods(self, mType, methodList, nb_method, nb_method_no_lfn):
        """Generates methods, read or write, and adds them to the appropriate list,
        including no_lfn if needed.
        The no_lfn methods are taken starting from the end.

        :param mType: read or write
        :param methodList: in which list to put the names
        :param nb_methods: number of methods
        :param nb_methods_no_lfn: number of these methods that should go in no lfn

        """

        no_lfn_start = nb_method - nb_method_no_lfn + 1
        for catId in range(1, nb_method + 1):
            mName = "%s%d" % (mType, catId)
            methodList.append(mName)
            if catId >= no_lfn_start:
                self.no_lfn.append(mName)

    def getInterfaceMethods(self):
        return self.r_method, self.w_method, self.no_lfn

    def generic(self, *args, **kwargs):
        """Returns a status depending on the input.
        For a normal read or write method, it looks for the catalog
        name in the LFN. If it is there, it looks at which status it is
        supposed to return: S_Error, or put the LFN in the Failed dict.
        """

        successful = {}
        failed = {}

        if self.call in self.no_lfn:
            if not args:
                return S_OK("yeah")
            ret = args[0]
            if self.name in ret:
                return S_ERROR(f"{self.name}.{self.call} did not go well")
            else:
                return S_OK("yeah")

        lfns = args[0]

        for lfn in lfns:
            lfnSplit = lfn.split("/")
            try:
                idName = lfnSplit.index(self.name)
                retType = lfnSplit[idName + 1]
                if retType == "Error":
                    return S_ERROR(f"{self.name}.{self.call} did not go well")
                elif retType == "Failed":
                    failed[lfn] = f"{self.name}.{self.call} failed for {lfn}"
            except ValueError:
                successful[lfn] = "yeah"

        return S_OK({"Successful": successful, "Failed": failed})

    def __getattr__(self, meth):
        self.call = meth
        return self.generic


def mock_fc_getSelectedCatalogs(self, desiredCatalogs):
    """Mock the getSelectedCatalogs method
    The name of the catalog should contain the following info, separated by '_':
      * the name of the catalog
      * True or False if it is a Master
      * True or False for Read
      * True or False for Write
      * nb of read op
      * nb of read no lfn
      * nb of write op
      * nb of write no lfn
    """

    for catalogDescription in desiredCatalogs:
        name, master, read, write, nb_read, nb_read_no_lfn, nb_write, nb_write_no_lfn = catalogDescription.split("_")
        master = eval(master)
        read = eval(read)
        write = eval(write)
        nb_read = eval(nb_read)
        nb_read_no_lfn = eval(nb_read_no_lfn)
        nb_write = eval(nb_write)
        nb_write_no_lfn = eval(nb_write_no_lfn)

        obj = GenericCatalog(name, nb_read, nb_read_no_lfn, nb_write, nb_write_no_lfn)

        if read:
            self.readCatalogs.append((name, obj, master))
        if write:
            self.writeCatalogs.append((name, obj, master))

    return S_OK()


def mock_fc_getEligibleCatalogs(self):
    """We return an object that always returns True
    if we ask whether an item is in it
    """

    class mockList:
        def __contains__(self, item):
            return True

    x = mockList()
    return S_OK(x)


def writeList(count, reverse=None):
    """If reverse is none, returns write1, ...., write<count>
    if reverse is set, returns a list with <count> elements backward from read<reverse>
    """
    if reverse:
        return ["write%s" % i for i in range(reverse, reverse - count, -1)]
    return ["write%s" % i for i in range(1, count + 1)]


def readList(count, reverse=None):
    """If reverse is none, returns read1, ...., read<count>
    if reverse is set, returns a list with <count> elements backward from read<reverse>
    """
    if reverse:
        return ["read%s" % i for i in range(reverse, reverse - count, -1)]

    return ["read%s" % i for i in range(1, count + 1)]


class TestInitialization(unittest.TestCase):
    """Tests the logic of the init mechanism"""

    @mock.patch.object(
        DIRAC.Resources.Catalog.FileCatalog.FileCatalog,
        "_getSelectedCatalogs",
        side_effect=mock_fc_getSelectedCatalogs,
        autospec=True,
    )  # autospec is for the binding of the method...
    @mock.patch.object(
        DIRAC.Resources.Catalog.FileCatalog.FileCatalog,
        "_getEligibleCatalogs",
        side_effect=mock_fc_getEligibleCatalogs,
        autospec=True,
    )  # autospec is for the binding of the method...
    def test_01_init(self, mk_getSelectedCatalogs, mk_getEligibleCatalogs):
        """Check logic of init"""

        # We should not be able to have 2 masters
        twoMastersFc = FileCatalog(catalogs=["c1_True_True_True_5_2_2_0", "c2_True_True_True_5_2_2_0"])
        self.assertTrue(not twoMastersFc.isOK())

        # One master should be ok
        oneMasterFc = FileCatalog(catalogs=["c1_True_True_True_2_0_2_2", "c2_False_True_True_3_1_4_2"])
        self.assertTrue(oneMasterFc.isOK())

        # With a master, the write method should be the method of the master
        self.assertEqual(sorted(oneMasterFc.write_methods), writeList(2))
        # The read methods and no_lfn should be from all catalogs
        self.assertEqual(sorted(oneMasterFc.ro_methods), readList(3))

        # The no_lfns methods are from everywhere
        # write1 and write2 from c1
        # write3, write4, read3 from c2
        self.assertEqual(sorted(oneMasterFc.no_lfn_methods), sorted(readList(1, reverse=3) + writeList(4)))

        # No master should be ok
        noMasterFc = FileCatalog(catalogs=["c1_False_True_True_2_0_2_0", "c2_False_True_True_3_0_4_0"])
        self.assertTrue(oneMasterFc.isOK())
        # With no master, the write method should be from all catalogs
        self.assertEqual(sorted(noMasterFc.write_methods), writeList(4))
        # The read methods and no_lfn should be from all catalogs
        self.assertEqual(sorted(noMasterFc.ro_methods), readList(3))


class TestWrite(unittest.TestCase):
    """Tests of the w_execute method"""

    @mock.patch.object(
        DIRAC.Resources.Catalog.FileCatalog.FileCatalog,
        "_getSelectedCatalogs",
        side_effect=mock_fc_getSelectedCatalogs,
        autospec=True,
    )  # autospec is for the binding of the method...
    @mock.patch.object(
        DIRAC.Resources.Catalog.FileCatalog.FileCatalog,
        "_getEligibleCatalogs",
        side_effect=mock_fc_getEligibleCatalogs,
        autospec=True,
    )  # autospec is for the binding of the method...
    def test_01_Normal(self, mk_getSelectedCatalogs, mk_getEligibleCatalogs):
        """Test behavior with one master and only standard write methods"""

        fc = FileCatalog(catalogs=["c1_True_True_True_2_0_2_0", "c2_False_True_True_3_0_1_0"])

        # Test a write method which is not in the master catalog
        with self.assertRaises(AttributeError):
            fc.write4("/lhcb/toto")

        # Test a write method which works for everybody
        lfn = "/lhcb/toto"
        res = fc.write1(lfn)
        self.assertTrue(res["OK"])
        self.assertTrue(lfn in res["Value"]["Successful"])
        self.assertEqual(sorted(["c1", "c2"]), sorted(res["Value"]["Successful"][lfn]))
        self.assertTrue(not res["Value"]["Failed"])

        # Test a write method that only the master has
        lfn = "/lhcb/toto"
        res = fc.write2(lfn)
        self.assertTrue(res["OK"])
        self.assertTrue(lfn in res["Value"]["Successful"])
        self.assertEqual(["c1"], sorted(res["Value"]["Successful"][lfn]))
        self.assertTrue(not res["Value"]["Failed"])

        # Test a write method that makes an error for master
        # We should get an error
        lfn = "/lhcb/c1/Error"
        res = fc.write1(lfn)
        self.assertTrue(not res["OK"])

        # Test a write method that fails for master
        # The lfn should be in failed and only attempted for the master
        lfn = "/lhcb/c1/Failed"
        res = fc.write1(lfn)
        self.assertTrue(res["OK"])
        self.assertTrue(not res["Value"]["Successful"])
        self.assertEqual(["c1"], sorted(res["Value"]["Failed"][lfn]))

        # Test a write method that makes an error for non master
        # The lfn should be in failed for non master and successful for the master
        lfn = "/lhcb/c2/Error"
        res = fc.write1(lfn)
        self.assertTrue(res["OK"])
        self.assertEqual(["c1"], sorted(res["Value"]["Successful"][lfn]))
        self.assertEqual(["c2"], sorted(res["Value"]["Failed"][lfn]))

        # Test a write method that fails for non master
        # The lfn should be in failed for non master and successful for the master
        lfn = "/lhcb/c2/Failed"
        res = fc.write1(lfn)
        self.assertTrue(res["OK"])
        self.assertEqual(["c1"], sorted(res["Value"]["Successful"][lfn]))
        self.assertEqual(["c2"], sorted(res["Value"]["Failed"][lfn]))

    @mock.patch.object(
        DIRAC.Resources.Catalog.FileCatalog.FileCatalog,
        "_getSelectedCatalogs",
        side_effect=mock_fc_getSelectedCatalogs,
        autospec=True,
    )  # autospec is for the binding of the method...
    @mock.patch.object(
        DIRAC.Resources.Catalog.FileCatalog.FileCatalog,
        "_getEligibleCatalogs",
        side_effect=mock_fc_getEligibleCatalogs,
        autospec=True,
    )  # autospec is for the binding of the method...
    def test_02_condParser(self, mk_getSelectedCatalogs, mk_getEligibleCatalogs):
        """Test behavior of write methode when using FCConditionParser"""

        fc = FileCatalog(
            catalogs=["c1_True_True_True_2_0_2_0", "c2_False_True_True_3_0_1_0", "c3_False_True_True_3_0_1_0"]
        )

        # No condition for c3, so it should always pass
        fcConditions = {"c1": "Filename=find('c1_pass')", "c2": "Filename=find('c2_pass')"}

        # Everything pass everywhere
        lfn1 = "/lhcb/c1_pass/c2_pass/lfn1"
        lfn2 = "/lhcb/c1_pass/c2_pass/lfn2"
        res = fc.write1([lfn1, lfn2], fcConditions=fcConditions)
        self.assertTrue(res["OK"])
        self.assertEqual(sorted(res["Value"]["Successful"]), sorted([lfn1, lfn2]))
        self.assertEqual(sorted(res["Value"]["Successful"][lfn1]), sorted(["c1", "c2", "c3"]))
        self.assertEqual(sorted(res["Value"]["Successful"][lfn2]), sorted(["c1", "c2", "c3"]))
        self.assertTrue(not res["Value"]["Failed"])

        # Everything pass for the master, only lfn2 for c2
        lfn1 = "/lhcb/c1_pass/lfn1"
        lfn2 = "/lhcb/c1_pass/c2_pass/lfn2"
        res = fc.write1([lfn1, lfn2], fcConditions=fcConditions)
        self.assertTrue(res["OK"])
        self.assertEqual(sorted(res["Value"]["Successful"]), sorted([lfn1, lfn2]))
        self.assertEqual(sorted(res["Value"]["Successful"][lfn1]), ["c1", "c3"])
        self.assertEqual(sorted(res["Value"]["Successful"][lfn2]), sorted(["c1", "c2", "c3"]))
        self.assertTrue(not res["Value"]["Failed"])

        # One is not valid for the master, so we do nothing
        lfn1 = "/lhcb/c2_pass/lfn1"
        lfn2 = "/lhcb/c1_pass/c2_pass/lfn2"
        res = fc.write1([lfn1, lfn2], fcConditions=fcConditions)
        self.assertTrue(not res["OK"])

    @mock.patch.object(
        DIRAC.Resources.Catalog.FileCatalog.FileCatalog,
        "_getSelectedCatalogs",
        side_effect=mock_fc_getSelectedCatalogs,
        autospec=True,
    )  # autospec is for the binding of the method...
    @mock.patch.object(
        DIRAC.Resources.Catalog.FileCatalog.FileCatalog,
        "_getEligibleCatalogs",
        side_effect=mock_fc_getEligibleCatalogs,
        autospec=True,
    )  # autospec is for the binding of the method...
    def test_03_noLFN(self, mk_getSelectedCatalogs, mk_getEligibleCatalogs):
        """Test the no_lfn methods"""

        fc = FileCatalog(catalogs=["c1_True_True_True_2_0_2_1", "c2_False_True_True_3_0_2_1"])

        # all good
        res = fc.write2("/lhcb/toto")
        self.assertTrue(res["OK"])
        self.assertEqual(res["Value"], "yeah")

        # Fail in the master
        res = fc.write2("/lhcb/c1")
        self.assertTrue(not res["OK"])
        self.assertTrue("Value" not in res)

        # Fail in the non master
        res = fc.write2("/lhcb/c2")
        self.assertTrue(res["OK"])
        self.assertTrue("Value" in res)
        self.assertEqual(res["Value"], "yeah")


class TestRead(unittest.TestCase):
    """Tests of the w_execute method"""

    @mock.patch.object(
        DIRAC.Resources.Catalog.FileCatalog.FileCatalog,
        "_getSelectedCatalogs",
        side_effect=mock_fc_getSelectedCatalogs,
        autospec=True,
    )  # autospec is for the binding of the method...
    @mock.patch.object(
        DIRAC.Resources.Catalog.FileCatalog.FileCatalog,
        "_getEligibleCatalogs",
        side_effect=mock_fc_getEligibleCatalogs,
        autospec=True,
    )  # autospec is for the binding of the method...
    def test_01_oneMasterNormal(self, mk_getSelectedCatalogs, mk_getEligibleCatalogs):
        """Test behavior with one master and only standard read methods"""

        fc = FileCatalog(catalogs=["c1_True_True_True_2_0_2_0", "c2_False_True_True_3_0_1_0"])

        # Test a write method which is not in the master catalog
        with self.assertRaises(AttributeError):
            fc.write4("/lhcb/toto")

        # Test a write method which works for everybody
        lfn = "/lhcb/toto"
        res = fc.write1(lfn)
        self.assertTrue(res["OK"])
        self.assertTrue(lfn in res["Value"]["Successful"])
        self.assertEqual(sorted(["c1", "c2"]), sorted(res["Value"]["Successful"][lfn]))
        self.assertTrue(not res["Value"]["Failed"])

        # Test a write method that only the master has
        lfn = "/lhcb/toto"
        res = fc.write2(lfn)
        self.assertTrue(res["OK"])
        self.assertTrue(lfn in res["Value"]["Successful"])
        self.assertEqual(["c1"], sorted(res["Value"]["Successful"][lfn]))
        self.assertTrue(not res["Value"]["Failed"])

        # Test a write method that makes an error for master
        # We should get an error
        lfn = "/lhcb/c1/Error"
        res = fc.write1(lfn)
        self.assertTrue(not res["OK"])

        # Test a write method that fails for master
        # The lfn should be in failed and only attempted for the master
        lfn = "/lhcb/c1/Failed"
        res = fc.write1(lfn)
        self.assertTrue(res["OK"])
        self.assertTrue(not res["Value"]["Successful"])
        self.assertEqual(["c1"], sorted(res["Value"]["Failed"][lfn]))

        # Test a write method that makes an error for non master
        # The lfn should be in failed for non master and successful for the master
        lfn = "/lhcb/c2/Error"
        res = fc.write1(lfn)
        self.assertTrue(res["OK"])
        self.assertEqual(["c1"], sorted(res["Value"]["Successful"][lfn]))
        self.assertEqual(["c2"], sorted(res["Value"]["Failed"][lfn]))

        # Test a write method that fails for non master
        # The lfn should be in failed for non master and successful for the master
        lfn = "/lhcb/c2/Failed"
        res = fc.write1(lfn)
        self.assertTrue(res["OK"])
        self.assertEqual(["c1"], sorted(res["Value"]["Successful"][lfn]))
        self.assertEqual(["c2"], sorted(res["Value"]["Failed"][lfn]))


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestInitialization)
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestWrite))
    suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestRead))

    unittest.TextTestRunner(verbosity=2).run(suite)
