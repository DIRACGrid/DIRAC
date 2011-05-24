#!/usr/bin/env python

"""
CLI to edit RSS Config
"""
import sys
import cmd

from DIRAC.Core.Base import Script
Script.parseCommandLine(ignoreErrors = True)

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ResourceStatusSystem.PolicySystem.Configurations import\
    ValidSiteType, ValidServiceType, ValidResourceType, ValidStatus

from DIRAC.ResourceStatusSystem.Utilities.Utils import GetForm

class RSSConfigCmd(cmd.Cmd):

  S = None
  item  = ["user", "status", "frequency", "group", "policy"]
  items = ["users", "statuses", "frequencies", "groups", "policies"]
  ops   = ['add', 'del', 'modify']
  status = ValidStatus
  # TODO: Get the status list from SQL and not from the static list

  prompt = "RSS > "

  def __init__(self):
    cmd.Cmd.__init__(self)
    self.S = RPCClient("ResourceStatus/RSSConfiguration")

  def do_sql(self, line):
    """[DEBUG] Issue a SQL command directly to the DB. Not for
ordinary users... but who here is an ordinary user?"""
    res = self.S.messWithDB(line)
    if not res['OK']:
      print res
      print "\nRPC Call failed: %s. " % res['Message']
    else:
      print res
      print "\nMessed with DB ok. Now I hope you meant what you typed :p"

  def do_add(self, line):
    """Usage: add <type> <value>
type ::== ["user", "status", "frequency", "group", "policy"]
    """
    args = line.split()
    if args[0] == "user":
      for u in args[1:]:
        res = self.S.addUser(u)
        if not res['OK']:
          print "Failed to add user " + u + ": " + "RPC Call failed: %s" % res['Message']
        else:
          print "Added user " + u
    elif args[0] == "status":
      usage = "Usage: add <status(str)> <priority(int)>"
      if len(args) < 3:
        print usage
      else:
        try:
          res = self.S.addStatus(args[1], int(args[2]))
          if not res['OK']:
            print "Failed to add status " + args[1] + ": " + "RPC Call failed: %s" % res['Message']
          else:
            print "Added status " + args[1] + " with priority " + args[2]
        except ValueError:
          print usage

  def complete_add(self, text, _line, _begidx, _endidx):
    """Completion of the 'add' command"""
    return [i for i in self.item if i.lower().startswith(text.lower())]

  def do_del(self, line):
    """Usage: del <type> <value>
type ::== ["user", "status", "frequency", "group", "policy"]
    """
    args = line.split()
    if args[0] == "user":
      for u in args[1:]:
        res = self.S.delUser(u)
        if not res['OK']:
          print "Failed to del user " + u + ": " + "RPC Call failed: %s" % res['Message']
        else:
          print "Deleted user " + u
    elif args[0] == "status":
      for s in args[1:]:
        res = self.S.delStatus(s)
        if not res['OK']:
          print "Failed to del status " + s + ": " + "RPC Call failed: %s" % res['Message']
        else:
          print "Deleted status " + s

  def complete_del(self, text, _line, _begidx, _endidx):
    """Completion of the 'add' command"""
    return [i for i in self.item if i.lower().startswith(text.lower())]

  def do_list(self, line):
    """Usage: list <type>
type ::== ["users", "statuses", "frequencies", "groups", "policies"]
    """
    if line == "users":
      res = self.S.getUsers()

    elif line == "statuses":
      res = self.S.getStatuses()

    else:
      print self.do_list.__doc__
      return

    if not res['OK']:
      print "RPC Call failed: %s" % res['Message']
    else:
      for u in res['Value']:
        if line == "users":
          print u['login'][0]
        elif line == "statuses":
          print u['label'] + "\t" + str(u['priority'])

  def complete_list(self, text, _line, _begidx, _endidx):
    """Completion of the 'list' command"""
    return [i for i in self.items if i.lower().startswith(text.lower())]

  def do_show(self, line):
    "See list"
    return self.do_list(line)

  def complete_show(self, text, _line, _begidx, _endidx):
    """Completion of the 'show' command"""
    return self.complete_list(text, _line, _begidx, _endidx)

  def do_policy(self, line):
    """Manage policies. Subcommands: add, del, modify"""
    args = line.split()
    if len(args) == 0:
      print "Usage: %s <action>\n<action> ::== add | del | modify"

    elif len(args) == 1:
      if args[0] == "add":
        request_dict = { 'label': str, 'description': str, 'status':ValidStatus,
                         'former_status':ValidStatus, 'site_type':ValidSiteType,
                         'service_type':ValidServiceType, 'resource_type':ValidResourceType }
        res = GetForm(request_dict).run()

        ret = self.S.addPolicy(res)

        if not ret['OK']:
          print "RPC Call failed: %s" % ret['Message']
          print str(ret)
        else:
          print "Policy added: " + str(ret)
      elif args[0] == "del":
        request_dict = { 'label': str, 'description': str, 'status':ValidStatus,
                         'former_status':ValidStatus, 'site_type':ValidSiteType,
                         'service_type':ValidServiceType, 'resource_type':ValidResourceType }
        res = GetForm(request_dict).run()
        ret = self.S.delPolicy(res)
        if not ret['OK']:
          print "RPC Call failed: %s" % ret['Message']
          print str(ret)
        else:
          print "Policy deleted: " + str(ret)

  def do_test(self, line):
    print line

  def complete_policy(self, text, line, _begidx, _endidx):
    args = line.split()
    if len(args) == 2:
      return [i for i in self.ops if i.lower().startswith(text.lower())]
    elif len(args) == 3:
      if args[1] == 'add':
        # Label, nothing to complete
        pass
      elif args[1] == 'del':
        # Get the list of policies labels to complete
        pass
      elif args[1] == 'modify':
        # TODO later
        pass

  def default(self, line):
    """Override [Cmd.default(line)] function."""
    if line == "EOF":
      if self.prompt:
        print
      return self.do_quit(line)
    else:
      cmd.Cmd.default(self, line)

  def do_quit(self, _line):
    """quit
    Quit"""
    return True

  def do_exit(self, line):
    """exit
    Quit"""
    return self.do_quit(line)



def main():
  rsscmd = RSSConfigCmd()
  rsscmd.cmdloop()

if __name__ == "__main__":
  sys.exit(main())
