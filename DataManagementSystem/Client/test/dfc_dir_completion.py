# TO-DO: to be moved to TestDIRAC

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

import cmd
import sys

from DIRAC.DataManagementSystem.Client.CmdDirCompletion.AbstractFileSystem import DFCFileSystem
from DIRAC.DataManagementSystem.Client.CmdDirCompletion.DirectoryCompletion import DirectoryCompletion

class DirCompletion(cmd.Cmd):

  fc = FileCatalogClient()
  dfc_fs = DFCFileSystem(fc)
  dc = DirectoryCompletion(dfc_fs)

  def do_exit(self, args):
    sys.exit(0)

  def _listdir(self, args):
    if self.dfc_fs.is_dir(args):
      return self.dfc_fs.list_dir(args)
    else:
      return [args]

  def _ls(self, args):
    try:
      return self._listdir(args)
    except:
      return []

  def do_ls(self, args):
    print 
    print " ".join(self._ls(args))

  def complete_ls(self, text, line, begidx, endidx):
    #print
    result = []

    cur_input_line = line.split()
    #print cur_input_line
    cur_path = "/"

    if (len(cur_input_line) == 2):
      cur_path = cur_input_line[1]
      #print "cur_path:", cur_path

    result = self.dc.parse_text_line(text, cur_path, "/" )

    return result

    

if __name__ == "__main__":


  cli = DirCompletion()
  cli.cmdloop()
