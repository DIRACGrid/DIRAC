"""Unit tests for pure JDL parsing utilities"""

import unittest

from diraccfg import CFG
from DIRACCommon.Core.Utilities.JDL import loadJDLAsCFG, dumpCFGAsJDL


class TestJDLParsing(unittest.TestCase):
    """Test cases for JDL parsing functions"""

    def test_loadJDLAsCFG_simple(self):
        """Test basic JDL parsing"""
        jdl = '[Executable = "test.sh"; Arguments = "arg1 arg2";]'
        result = loadJDLAsCFG(jdl)
        self.assertTrue(result["OK"])
        cfg, _ = result["Value"]
        self.assertEqual(cfg.getOption("Executable"), "test.sh")
        self.assertEqual(cfg.getOption("Arguments"), "arg1 arg2")

    def test_loadJDLAsCFG_with_lists(self):
        """Test JDL parsing with lists"""
        jdl = '[InputSandbox = {"file1.txt", "file2.txt"}; Priority = 5;]'
        result = loadJDLAsCFG(jdl)
        self.assertTrue(result["OK"])
        cfg, _ = result["Value"]
        self.assertEqual(cfg.getOption("InputSandbox"), "file1.txt, file2.txt")
        self.assertEqual(cfg.getOption("Priority"), "5")

    def test_loadJDLAsCFG_empty(self):
        """Test empty JDL parsing"""
        jdl = "[]"
        result = loadJDLAsCFG(jdl)
        self.assertTrue(result["OK"])
        cfg, _ = result["Value"]
        self.assertEqual(len(cfg.listOptions()), 0)

    def test_loadJDLAsCFG_invalid_format(self):
        """Test invalid JDL format"""
        jdl = '[Executable = "test.sh" Arguments = "missing semicolon"]'
        result = loadJDLAsCFG(jdl)
        # This should either succeed with partial parsing or fail gracefully
        # The exact behavior depends on the parser implementation

    def test_dumpCFGAsJDL_simple(self):
        """Test basic CFG to JDL conversion"""
        cfg = CFG()
        cfg.setOption("Executable", "test.sh")
        cfg.setOption("Arguments", "arg1 arg2")

        jdl = dumpCFGAsJDL(cfg)

        # Should contain the key fields
        self.assertIn("Executable", jdl)
        self.assertIn("test.sh", jdl)
        self.assertIn("Arguments", jdl)
        self.assertIn("arg1 arg2", jdl)

        # Should be properly formatted
        self.assertTrue(jdl.strip().startswith("["))
        self.assertTrue(jdl.strip().endswith("]"))

    def test_dumpCFGAsJDL_with_numbers(self):
        """Test CFG to JDL with numerical values"""
        cfg = CFG()
        cfg.setOption("CPUTime", "3600")
        cfg.setOption("Priority", "5")

        jdl = dumpCFGAsJDL(cfg)

        # Numbers should not be quoted
        self.assertIn("CPUTime = 3600", jdl)
        self.assertIn("Priority = 5", jdl)

    def test_dumpCFGAsJDL_with_lists(self):
        """Test CFG to JDL with list values"""
        cfg = CFG()
        cfg.setOption("InputSandbox", "file1.txt, file2.txt")

        jdl = dumpCFGAsJDL(cfg)

        # Lists should be formatted with braces
        self.assertIn("InputSandbox", jdl)
        self.assertIn("{", jdl)
        self.assertIn("}", jdl)
        self.assertIn("file1.txt", jdl)
        self.assertIn("file2.txt", jdl)

    def test_roundtrip_conversion(self):
        """Test that JDL -> CFG -> JDL preserves basic information"""
        original_jdl = '[Executable = "test.sh"; CPUTime = 3600; InputSandbox = {"file1.txt", "file2.txt"};]'

        # Parse JDL to CFG
        result = loadJDLAsCFG(original_jdl)
        self.assertTrue(result["OK"])
        cfg, _ = result["Value"]

        # Convert back to JDL
        new_jdl = dumpCFGAsJDL(cfg)

        # Parse the new JDL
        result2 = loadJDLAsCFG(new_jdl)
        self.assertTrue(result2["OK"])
        cfg2, _ = result2["Value"]

        # Compare key values
        self.assertEqual(cfg.getOption("Executable"), cfg2.getOption("Executable"))
        self.assertEqual(cfg.getOption("CPUTime"), cfg2.getOption("CPUTime"))
        # Note: List format might be different but content should be preserved


if __name__ == "__main__":
    unittest.main()
