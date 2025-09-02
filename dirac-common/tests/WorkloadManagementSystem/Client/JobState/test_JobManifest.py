"""Test the JobManifest class."""

import unittest

from DIRACCommon.WorkloadManagementSystem.Client.JobState.JobManifest import JobManifest


class TestJobManifest(unittest.TestCase):
    """Test cases for JobManifest"""

    def test_create_empty_manifest(self):
        """Test creating an empty manifest"""
        manifest = JobManifest()
        self.assertFalse(manifest.isDirty())

    def test_load_simple_jdl(self):
        """Test loading a simple JDL"""
        jdl = '[Executable = "test.sh"; Arguments = "arg1";]'
        manifest = JobManifest(jdl)

        self.assertEqual(manifest.getOption("Executable"), "test.sh")
        self.assertEqual(manifest.getOption("Arguments"), "arg1")

    def test_load_cfg_format(self):
        """Test loading CFG format"""
        cfg_content = """Executable = test.sh
Arguments = arg1 arg2
CPUTime = 3600"""

        manifest = JobManifest()
        result = manifest.loadCFG(cfg_content)
        self.assertTrue(result["OK"])

        self.assertEqual(manifest.getOption("Executable"), "test.sh")
        self.assertEqual(manifest.getOption("Arguments"), "arg1 arg2")
        self.assertEqual(manifest.getOption("CPUTime"), "3600")

    def test_set_and_get_options(self):
        """Test setting and getting options"""
        manifest = JobManifest()

        manifest.setOption("TestOption", "TestValue")
        self.assertTrue(manifest.isDirty())

        self.assertEqual(manifest.getOption("TestOption"), "TestValue")

    def test_dump_formats(self):
        """Test dumping to different formats"""
        manifest = JobManifest()
        manifest.setOption("Executable", "test.sh")
        manifest.setOption("CPUTime", "3600")

        # Test CFG format
        cfg_output = manifest.dumpAsCFG()
        self.assertIn("Executable", cfg_output)
        self.assertIn("test.sh", cfg_output)

        # Test JDL format
        jdl_output = manifest.dumpAsJDL()
        self.assertIn("Executable", jdl_output)
        self.assertIn("test.sh", jdl_output)
        self.assertTrue(jdl_output.strip().startswith("["))
        self.assertTrue(jdl_output.strip().endswith("]"))

    def test_contains_operator(self):
        """Test the __contains__ operator"""
        manifest = JobManifest()
        manifest.setOption("TestKey", "TestValue")

        self.assertTrue("TestKey" in manifest)
        self.assertFalse("NonExistentKey" in manifest)

    def test_create_and_get_section(self):
        """Test creating and getting sections"""
        manifest = JobManifest()

        result = manifest.createSection("TestSection")
        self.assertTrue(result["OK"])

        result = manifest.getSection("TestSection")
        self.assertTrue(result["OK"])

        # Try to create same section again - should fail
        result = manifest.createSection("TestSection")
        self.assertFalse(result["OK"])

    def test_option_list_operations(self):
        """Test getting lists of options and sections"""
        manifest = JobManifest()
        manifest.setOption("Option1", "Value1")
        manifest.setOption("Option2", "Value2")
        manifest.createSection("Section1")

        options = manifest.getOptionList()
        self.assertIn("Option1", options)
        self.assertIn("Option2", options)

        sections = manifest.getSectionList()
        self.assertIn("Section1", sections)

    def test_remove_option(self):
        """Test removing options"""
        manifest = JobManifest()
        manifest.setOption("TestOption", "TestValue")

        self.assertTrue("TestOption" in manifest)

        result = manifest.remove("TestOption")
        self.assertTrue(result["OK"])

        self.assertFalse("TestOption" in manifest)

    def test_dirty_flag_management(self):
        """Test dirty flag management"""
        manifest = JobManifest()
        self.assertFalse(manifest.isDirty())

        manifest.setOption("Test", "Value")
        self.assertTrue(manifest.isDirty())

        manifest.clearDirty()
        self.assertFalse(manifest.isDirty())

        manifest.setDirty()
        self.assertTrue(manifest.isDirty())


if __name__ == "__main__":
    unittest.main()
