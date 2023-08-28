# TO-DO: to be moved to tests directory

import cmd
import sys
import os.path

from DIRAC.DataManagementSystem.Client.CmdDirCompletion.AbstractFileSystem import UnixLikeFileSystem
from DIRAC.DataManagementSystem.Client.CmdDirCompletion.DirectoryCompletion import DirectoryCompletion


class DirCompletion(cmd.Cmd):
    ulfs = UnixLikeFileSystem()
    dc = DirectoryCompletion(ulfs)

    def do_exit(self, args):
        sys.exit(0)

    def _listdir(self, args):
        if os.path.isdir(args):
            return os.listdir(args)
        else:
            return [args]

    def _ls(self, args):
        try:
            return self._listdir(args)
        except:
            return []

    def do_ls(self, args):
        print()
        print(" ".join(self._ls(args)))

    def complete_ls(self, text, line, begidx, endidx):
        # print
        result = []

        cur_input_line = line.split()
        cur_path = "."
        if len(cur_input_line) == 2:
            cur_path = cur_input_line[1]
            # print "cur_path:", cur_path

        result = self.dc.parse_text_line(text, cur_path, os.getcwd())

        return result


if __name__ == "__main__":
    cli = DirCompletion()
    cli.cmdloop()
