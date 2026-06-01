""" Test the FilenamePlugin class"""
import unittest
from DIRAC.Resources.Catalog.ConditionPlugins.FilenamePlugin import FilenamePlugin


class TestfilenamePlugin(unittest.TestCase):
    """Test the FilenamePlugin class"""

    def setUp(self):
        self.lfns = ["/lhcb/lfn1", "/lhcb/anotherlfn", "/otherVo/name"]

    def test_01_endswith(self):
        """Testing endswith (method with argument"""

        fnp = FilenamePlugin("endswith('n')")

        self.assertTrue(not fnp.eval(lfn="/lhcb/lfn1"))
        self.assertTrue(fnp.eval(lfn="/lhcb/lfn"))

    def test_02_find(self):
        """Testing special case of find"""

        fnp = FilenamePlugin("find('lfn')")

        self.assertTrue(fnp.eval(lfn="/lhcb/lfn1"))
        self.assertTrue(not fnp.eval(lfn="/lhcb/l0f0n"))

    def test_03_isalnum(self):
        """Testing isalnum (method without argument"""

        fnp = FilenamePlugin("isalnum()")

        self.assertTrue(fnp.eval(lfn="lhcblfn1"))
        self.assertTrue(not fnp.eval(lfn="/lhcb/lf_n"))

    def test_04_nonExisting(self):
        """Testing non-existing method name is rejected at init time"""

        with self.assertRaises(ValueError):
            FilenamePlugin("nonexisting()")

    def test_05_startswith(self):
        """Testing startswith (method with argument)"""

        fnp = FilenamePlugin("startswith('/lhcb')")

        self.assertTrue(fnp.eval(lfn="/lhcb/lfn1"))
        self.assertTrue(not fnp.eval(lfn="/otherVo/name"))

    def test_06_isalpha(self):
        """Testing isalpha"""

        fnp = FilenamePlugin("isalpha()")

        self.assertTrue(fnp.eval(lfn="lhcblfn"))
        self.assertTrue(not fnp.eval(lfn="/lhcb/lfn1"))

    def test_07_isdigit(self):
        """Testing isdigit"""

        fnp = FilenamePlugin("isdigit()")

        self.assertTrue(fnp.eval(lfn="12345"))
        self.assertTrue(not fnp.eval(lfn="abcde"))

    def test_08_islower(self):
        """Testing islower"""

        fnp = FilenamePlugin("islower()")

        self.assertTrue(fnp.eval(lfn="abc"))
        self.assertTrue(not fnp.eval(lfn="ABC"))

    def test_09_isspace(self):
        """Testing isspace"""

        fnp = FilenamePlugin("isspace()")

        self.assertTrue(fnp.eval(lfn="   "))
        self.assertTrue(not fnp.eval(lfn="hello"))

    def test_10_istitle(self):
        """Testing istitle"""

        fnp = FilenamePlugin("istitle()")

        self.assertTrue(fnp.eval(lfn="Hello World"))
        self.assertTrue(not fnp.eval(lfn="hello world"))

    def test_11_isupper(self):
        """Testing isupper"""

        fnp = FilenamePlugin("isupper()")

        self.assertTrue(fnp.eval(lfn="HELLO"))
        self.assertTrue(not fnp.eval(lfn="Hello"))

    def test_12_find_with_bounds(self):
        """Testing find with start and end arguments"""

        fnp = FilenamePlugin("find('lfn', 0, 10)")

        self.assertTrue(fnp.eval(lfn="/lhcb/lfn1"))
        self.assertTrue(not fnp.eval(lfn="/otherVo/name"))

    def test_13_endswith_with_bounds(self):
        """Testing endswith with start argument"""

        fnp = FilenamePlugin("endswith('n', 0, 10)")

        self.assertTrue(not fnp.eval(lfn="/lhcb/lfn1"))
        self.assertTrue(fnp.eval(lfn="/lhcb/lfn"))

    def test_14_startswith_with_bounds(self):
        """Testing startswith with start/end arguments"""

        fnp = FilenamePlugin("startswith('/lhcb', 0, 7)")

        self.assertTrue(fnp.eval(lfn="/lhcb/lfn1"))
        self.assertTrue(not fnp.eval(lfn="/otherVo/name"))

    def test_15_long_expression(self):
        """Testing that overly long expressions are rejected"""

        with self.assertRaises(ValueError):
            FilenamePlugin("endswith('" + "a" * 130 + "')")

    def test_16_code_injection_semicolon(self):
        """Testing that semicolon-based code injection is rejected"""

        with self.assertRaises(ValueError):
            FilenamePlugin("endswith(x);__import__('os').system('id')")

    def test_17_code_injection_double_attr(self):
        """Testing that __class__ chain injection is rejected"""

        with self.assertRaises(ValueError):
            FilenamePlugin("endswith(x).__class__.__mro__")

    def test_18_code_injection_getattr(self):
        """Testing that getattr-based injection is rejected"""

        with self.assertRaises(ValueError):
            FilenamePlugin("getattr(str, 'replace')")

    def test_19_non_call_expression_list(self):
        """Testing that list literals are rejected"""

        with self.assertRaises(ValueError):
            FilenamePlugin("[1, 2, 3]")

    def test_20_non_call_expression_lambda(self):
        """Testing that lambda expressions are rejected"""

        with self.assertRaises(ValueError):
            FilenamePlugin("lambda x: x")

    def test_21_injection_in_function_name(self):
        """Testing that dangerous function names are rejected"""

        with self.assertRaises(ValueError):
            FilenamePlugin("__import__('os').system('id')")

    def test_22_empty_condition(self):
        """Testing empty/None lfn returns False"""

        fnp = FilenamePlugin("isalnum()")

        self.assertFalse(fnp.eval())  # no lfn
        self.assertFalse(fnp.eval(lfn=None))  # None lfn

    def test_23_special_characters_in_lfn(self):
        """Testing LFNs with special characters"""

        fnp = FilenamePlugin("startswith('/lhcb')")

        self.assertTrue(fnp.eval(lfn="/lhcb/data/lfn with spaces"))
        self.assertTrue(fnp.eval(lfn="/lhcb/data/lfn=with=equals"))

    def test_24_mixed_case_injection(self):
        """Testing mixed-case injection attempts are rejected"""

        with self.assertRaises(ValueError):
            FilenamePlugin("ENDSWITH(x);import os")

    def test_25_injection_via_kwargs(self):
        """Testing injection attempted via keyword arguments"""

        with self.assertRaises(ValueError):
            FilenamePlugin("endswith(suffix='x');__import__('os')")

    def test_26_find_at_position_zero(self):
        """find() returns 0 for a match at index 0 — must evaluate to True"""

        fnp = FilenamePlugin("find('lfn')")
        self.assertTrue(fnp.eval(lfn="/lhcb/lfn1"))

    def test_27_find_not_found(self):
        """find() returns -1 when not found — must evaluate to False"""

        fnp = FilenamePlugin("find('z')")
        self.assertFalse(fnp.eval(lfn="abc"))

    def test_28_istitle_mixed(self):
        """istitle() edge cases"""

        fnp = FilenamePlugin("istitle()")
        self.assertTrue(fnp.eval(lfn="Hello"))
        self.assertFalse(fnp.eval(lfn="hello"))
        self.assertFalse(fnp.eval(lfn="HELLO"))

    def test_29_slicing_not_supported(self):
        """Testing that list slicing expressions are rejected"""

        with self.assertRaises(ValueError):
            FilenamePlugin("[0:5]")

    def test_30_subscript_not_supported(self):
        """Testing that subscript expressions are rejected"""

        with self.assertRaises(ValueError):
            FilenamePlugin("x[0]")

    def test_31_double_underscore(self):
        """Testing that double-underscore function names are rejected"""

        with self.assertRaises(ValueError):
            FilenamePlugin("__init__()")


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestfilenamePlugin)

    unittest.TextTestRunner(verbosity=2).run(suite)
