""" Transformation Database Client Command Line Interface.
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.Core.Base.CLI import CLI
from DIRAC.Core.Base.API import API
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

__RCSID__ = "$Id$"


def printDict(dictionary):
  """ Dictionary pretty printing """
  key_max = 0
  value_max = 0
  for key, value in dictionary.items():
    if len(key) > key_max:
      key_max = len(key)
    if len(str(value)) > value_max:
      value_max = len(str(value))
  for key, value in dictionary.items():
    print(key.rjust(key_max), ' : ', str(value).ljust(value_max))


class TransformationCLI(CLI, API):

  def __init__(self):
    self.transClient = TransformationClient()
    self.indentSpace = 4
    CLI.__init__(self)
    API.__init__(self)

  def printPair(self, key, value, separator=":"):
    valueList = value.split("\n")
    print("%s%s%s %s" % (key, " " * (self.indentSpace - len(key)), separator, valueList[0].strip()))
    for valueLine in valueList[1:-1]:
      print("%s  %s" % (" " * self.indentSpace, valueLine.strip()))

  def do_help(self, args):
    """ Default version of the help command
       Usage: help <command>
       OR use helpall to see description for all commands"""
    CLI.do_help(self, args)

  # overriting default help command
  def do_helpall(self, args):
    """
    Shows help information
        Usage: helpall <command>
        If no command is specified all commands are shown
    """
    if len(args) == 0:
      print("\nAvailable commands:\n")
      attrList = sorted(dir(self))
      for attribute in attrList:
        if attribute.find("do_") == 0:
          self.printPair(attribute[3:], getattr(self, attribute).__doc__[1:])
          print("")
    else:
      command = args.split()[0].strip()
      try:
        obj = getattr(self, "do_%s" % command)
      except Exception:
        print("There's no such %s command" % command)
        return
      self.printPair(command, obj.__doc__[1:])

  def do_shell(self, args):
    """Execute a shell command

       usage !<shell_command>
    """
    comm = args
    res = shellCall(0, comm)
    if res['OK'] and res['Value'][0] == 0:
      _returnCode, stdOut, stdErr = res['Value']
      print("%s\n%s" % (stdOut, stdErr))
    else:
      print(res['Message'])

  def check_params(self, args, num):
    """Checks if the number of parameters correct"""
    argss = args.split()
    length = len(argss)
    if length < num:
      print("Error: Number of arguments provided %d less that required %d, please correct." % (length, num))
      return (False, length)
    return (argss, length)

  def check_id_or_name(self, id_or_name):
    """resolve name or Id by converting type of argument """
    if id_or_name.isdigit():
      return int(id_or_name)  # its look like id
    return id_or_name

  ####################################################################
  #
  # These are the methods for transformation manipulation
  #

  def do_getall(self, args):
    """Get transformation details

       usage: getall [Status] [Status]
    """
    oTrans = Transformation()
    oTrans.getTransformations(transStatus=args.split(), printOutput=True)

  def do_getAllByUser(self, args):
    """Get all transformations created by a given user

The first argument is the authorDN or username. The authorDN
is preferred: it need to be inside quotes because contains
white spaces. Only authorDN should be quoted.

When the username is provided instead,
the authorDN is retrieved from the uploaded proxy,
so that the retrieved transformations are those created by
the user who uploaded that proxy: that user could be different
that the username provided to the function.

       usage: getAllByUser authorDN or username [Status] [Status]
    """
    oTrans = Transformation()
    argss = args.split()
    username = ""
    author = ""
    status = []
    if not len(argss) > 0:
      print(self.do_getAllByUser.__doc__)
      return

    # if the user didnt quoted the authorDN ends
    if '=' in argss[0] and argss[0][0] not in ["'", '"']:
      print("AuthorDN need to be quoted (just quote that argument)")
      return

    if argss[0][0] in ["'", '"']:  # authorDN given
      author = argss[0]
      status_idx = 1
      for arg in argss[1:]:
        author += ' ' + arg
        status_idx += 1
        if arg[-1] in ["'", '"']:
          break
      # At this point we should have something like 'author'
      if not author[0] in ["'", '"'] or not author[-1] in ["'", '"']:
        print("AuthorDN need to be quoted (just quote that argument)")
        return
      else:
        author = author[1:-1]  # throw away the quotes
      # the rest are the requested status
      status = argss[status_idx:]
    else:  # username given
      username = argss[0]
      status = argss[1:]

    oTrans.getTransformationsByUser(authorDN=author, userName=username, transStatus=status, printOutput=True)

  def do_summaryTransformations(self, args):
    """Show the summary for a list of Transformations

    Fields starting with 'F' ('J')  refers to files (jobs).
    Proc. stand for processed.

        Usage: summaryTransformations <ProdID> [<ProdID> ...]
    """
    argss = args.split()
    if not len(argss) > 0:
      print(self.do_summaryTransformations.__doc__)
      return

    transid = argss
    oTrans = Transformation()
    oTrans.getSummaryTransformations(transID=transid)

  def do_getStatus(self, args):
    """Get transformation details

       usage: getStatus <transName|ID>
    """
    argss = args.split()
    if not len(argss) > 0:
      print("no transformation supplied")
      return
    for transName in argss:
      res = self.transClient.getTransformation(transName)
      if not res['OK']:
        print("Getting status of %s failed: %s" % (transName, res['Message']))
      else:
        print("%s: %s" % (transName, res['Value']['Status']))

  def do_setStatus(self, args):
    """Set transformation status

       usage: setStatus  <Status> <transName|ID>
       Status <'New' 'Active' 'Stopped' 'Completed' 'Cleaning'>
    """
    argss = args.split()
    if not len(argss) > 1:
      print("transformation and status not supplied")
      return
    status = argss[0]
    transNames = argss[1:]
    for transName in transNames:
      res = self.transClient.setTransformationParameter(transName, 'Status', status)
      if not res['OK']:
        print("Setting status of %s failed: %s" % (transName, res['Message']))
      else:
        print("%s set to %s" % (transName, status))

  def do_start(self, args):
    """Start transformation

       usage: start <transName|ID>
    """
    argss = args.split()
    if not len(argss) > 0:
      print("no transformation supplied")
      return
    for transName in argss:
      res = self.transClient.setTransformationParameter(transName, 'Status', 'Active')
      if not res['OK']:
        print("Setting Status of %s failed: %s" % (transName, res['Message']))
      else:
        res = self.transClient.setTransformationParameter(transName, 'AgentType', 'Automatic')
        if not res['OK']:
          print("Setting AgentType of %s failed: %s" % (transName, res['Message']))
        else:
          print("%s started" % transName)

  def do_stop(self, args):
    """Stop transformation

       usage: stop <transID|ID>
    """
    argss = args.split()
    if not len(argss) > 0:
      print("no transformation supplied")
      return
    if not len(argss) > 0:
      print("no transformation supplied")
      return
    for transName in argss:
      res = self.transClient.setTransformationParameter(transName, 'AgentType', 'Manual')
      if not res['OK']:
        print("Stopping of %s failed: %s" % (transName, res['Message']))
      else:
        print("%s stopped" % transName)

  def do_flush(self, args):
    """Flush transformation

       usage: flush <transName|ID>
    """
    argss = args.split()
    if not len(argss) > 0:
      print("no transformation supplied")
      return
    for transName in argss:
      res = self.transClient.setTransformationParameter(transName, 'Status', 'Flush')
      if not res['OK']:
        print("Flushing of %s failed: %s" % (transName, res['Message']))
      else:
        print("%s flushing" % transName)

  def do_get(self, args):
    """Get transformation definition

    usage: get <transName|ID>
    """
    argss = args.split()
    if not len(argss) > 0:
      print("no transformation supplied")
      return
    transName = argss[0]
    res = self.transClient.getTransformation(transName)
    if not res['OK']:
      print("Failed to get %s: %s" % (transName, res['Message']))
    else:
      res['Value'].pop('Body')
      printDict(res['Value'])

  def do_getBody(self, args):
    """Get transformation body

    usage: getBody <transName|ID>
    """
    argss = args.split()
    if not len(argss) > 0:
      print("no transformation supplied")
      return
    transName = argss[0]
    res = self.transClient.getTransformation(transName)
    if not res['OK']:
      print("Failed to get %s: %s" % (transName, res['Message']))
    else:
      print(res['Value']['Body'])

  def do_getFileStat(self, args):
    """Get transformation file statistics

     usage: getFileStat <transName|ID>
    """
    argss = args.split()
    if not len(argss) > 0:
      print("no transformation supplied")
      return
    transName = argss[0]
    res = self.transClient.getTransformationStats(transName)
    if not res['OK']:
      print("Failed to get statistics for %s: %s" % (transName, res['Message']))
    else:
      res['Value'].pop('Total')
      printDict(res['Value'])

  def do_modMask(self, args):
    """Modify transformation input definition

       usage: modInput <mask> <transName|ID>
    """
    argss = args.split()
    if not len(argss) > 0:
      print("no transformation supplied")
      return
    mask = argss[0]
    transNames = argss[1:]
    for transName in transNames:
      res = self.transClient.setTransformationParameter(transName, "FileMask", mask)
      if not res['OK']:
        print("Failed to modify input file mask for %s: %s" % (transName, res['Message']))
      else:
        print("Updated %s filemask" % transName)

  def do_getFiles(self, args):
    """Get files for the transformation (optionally with a given status)

    usage: getFiles <transName|ID> [Status] [Status]
    """
    argss = args.split()
    if not len(argss) > 0:
      print("no transformation supplied")
      return
    transName = argss[0]
    status = argss[1:]
    res = self.transClient.getTransformation(transName)
    if not res['OK']:
      print("Failed to get transformation information: %s" % res['Message'])
    else:
      selectDict = {'TransformationID': res['Value']['TransformationID']}
      if status:
        selectDict['Status'] = status
      res = self.transClient.getTransformationFiles(condDict=selectDict)
      if not res['OK']:
        print("Failed to get transformation files: %s" % res['Message'])
      elif res['Value']:
        self._printFormattedDictList(res['Value'], ['LFN', 'Status', 'ErrorCount', 'TargetSE', 'LastUpdate'],
                                     'LFN', 'LFN')
      else:
        print("No files found")

  def do_getFileStatus(self, args):
    """Get file(s) status for the given transformation

    usage: getFileStatus <transName|ID> <lfn> [<lfn>...]
    """
    argss = args.split()
    if len(argss) < 2:
      print("transformation and file not supplied")
      return
    transName = argss[0]
    lfns = argss[1:]

    res = self.transClient.getTransformation(transName)
    if not res['OK']:
      print("Failed to get transformation information: %s" % res['Message'])
    else:
      selectDict = {'TransformationID': res['Value']['TransformationID']}
      res = self.transClient.getTransformationFiles(condDict=selectDict)
      if not res['OK']:
        print("Failed to get transformation files: %s" % res['Message'])
      elif res['Value']:
        filesList = []
        for fileDict in res['Value']:
          if fileDict['LFN'] in lfns:
            filesList.append(fileDict)
        if filesList:
          self._printFormattedDictList(filesList, ['LFN', 'Status', 'ErrorCount', 'TargetSE', 'LastUpdate'],
                                       'LFN', 'LFN')
        else:
          print("Could not find any LFN in", lfns, "for transformation", transName)
      else:
        print("No files found")

  def do_getOutputFiles(self, args):
    """Get output files for the transformation

    usage: getOutputFiles <transName|ID>
    """
    argss = args.split()
    if not len(argss) > 0:
      print("no transformation supplied")
      return
    transName = argss[0]
    res = self.transClient.getTransformation(transName)
    if not res['OK']:
      print("Failed to get transformation information: %s" % res['Message'])
    else:
      fc = FileCatalog()
      meta = {}
      meta['ProdID'] = transName
      res = fc.findFilesByMetadata(meta)
      if not res['OK']:
        print(res['Message'])
        return
      if not len(res['Value']) > 0:
        print('No output files yet for transformation %d' % int(transName))
        return
      else:
        for lfn in res['Value']:
          print(lfn)

  def do_getInputDataQuery(self, args):
    """Get input data query for the transformation

    usage: getInputDataQuery <transName|ID>
    """
    argss = args.split()
    if not len(argss) > 0:
      print("no transformation supplied")
      return
    transName = argss[0]
    # res = self.transClient.getTransformationInputDataQuery( transName )
    res = self.transClient.getTransformationMetaQuery(transName, 'Input')
    if not res['OK']:
      print("Failed to get transformation input data query: %s" % res['Message'])
    else:
      print(res['Value'])

  def do_setFileStatus(self, args):
    """Set file status for the given transformation

    usage: setFileStatus <transName|ID> <lfn> <status>
    """
    argss = args.split()
    if not len(argss) == 3:
      print("transformation file and status not supplied")
      return
    transName = argss[0]
    lfn = argss[1]
    status = argss[2]
    res = self.transClient.setFileStatusForTransformation(transName, status, [lfn])
    if not res['OK']:
      print("Failed to update file status: %s" % res['Message'])
    else:
      print("Updated file status to %s" % status)

  def do_resetFile(self, args):
    """Reset file status for the given transformation

    usage: resetFile <transName|ID> <lfns>
    """
    argss = args.split()
    if not len(argss) > 1:
      print("transformation and file(s) not supplied")
      return
    transName = argss[0]
    lfns = argss[1:]
    res = self.transClient.setFileStatusForTransformation(transName, 'Unused', lfns)
    if not res['OK']:
      print("Failed to reset file status: %s" % res['Message'])
    else:
      if 'Failed' in res['Value']:
        print("Could not reset some files: ")
        for lfn, reason in res['Value']['Failed'].items():
          print(lfn, reason)
      else:
        print("Updated file statuses to 'Unused' for %d file(s)" % len(lfns))

  def do_resetProcessedFile(self, args):
    """ Reset file status for the given transformation
        usage: resetFile <transName|ID> <lfn>
    """
    argss = args.split()

    if not len(argss) > 1:
      print("transformation and file(s) not supplied")
      return
    transName = argss[0]
    lfns = argss[1:]
    res = self.transClient.setFileStatusForTransformation(transName, 'Unused', lfns, force=True)
    if not res['OK']:
      print("Failed to reset file status: %s" % res['Message'])
    else:
      if 'Failed' in res['Value'] and res['Value']['Failed']:
        print("Could not reset some files: ")
        for lfn, reason in res['Value']['Failed'].items():
          print(lfn, reason)
      else:
        print("Updated file statuses to 'Unused' for %d file(s)" % len(lfns))

  ####################################################################
  #
  # These are the methods for file manipulation
  #

  def do_addDirectory(self, args):
    """Add files from the given catalog directory

    usage: addDirectory <directory> [directory]
    """
    argss = args.split()
    if not len(argss) > 0:
      print("no directory supplied")
      return
    for directory in argss:
      res = self.transClient.addDirectory(directory, force=True)
      if not res['OK']:
        print('failed to add directory %s: %s' % (directory, res['Message']))
      else:
        print('added %s files for %s' % (res['Value'], directory))

  def do_replicas(self, args):
    """ Get replicas for <path>

        usage: replicas <lfn> [lfn]
    """
    argss = args.split()
    if not len(argss) > 0:
      print("no files supplied")
      return
    res = self.transClient.getReplicas(argss)
    if not res['OK']:
      print("failed to get any replica information: %s" % res['Message'])
      return
    for lfn in sorted(res['Value']['Failed']):
      error = res['Value']['Failed'][lfn]
      print("failed to get replica information for %s: %s" % (lfn, error))
    for lfn in sorted(res['Value']['Successful']):
      ses = sorted(res['Value']['Successful'][lfn])
      outStr = "%s :" % lfn.ljust(100)
      for se in ses:
        outStr = "%s %s" % (outStr, se.ljust(15))
      print(outStr)

  def do_addFile(self, args):
    """Add new files to transformation DB

    usage: addFile <lfn> [lfn]
    """
    argss = args.split()
    if not len(argss) > 0:
      print("no files supplied")
      return
    lfnDict = {}
    for lfn in argss:
      lfnDict[lfn] = {'PFN': 'IGNORED-PFN', 'SE': 'IGNORED-SE', 'Size': 0, 'GUID': 'IGNORED-GUID',
                      'Checksum': 'IGNORED-CHECKSUM'}
    res = self.transClient.addFile(lfnDict, force=True)
    if not res['OK']:
      print("failed to add any files: %s" % res['Message'])
      return
    for lfn in sorted(res['Value']['Failed']):
      error = res['Value']['Failed'][lfn]
      print("failed to add %s: %s" % (lfn, error))
    for lfn in sorted(res['Value']['Successful']):
      print("added %s" % lfn)

  def do_removeFile(self, args):
    """Remove file from transformation DB

    usage: removeFile <lfn> [lfn]
    """
    argss = args.split()
    if not len(argss) > 0:
      print("no files supplied")
      return
    res = self.transClient.removeFile(argss)
    if not res['OK']:
      print("failed to remove any files: %s" % res['Message'])
      return
    for lfn in sorted(res['Value']['Failed']):
      error = res['Value']['Failed'][lfn]
      print("failed to remove %s: %s" % (lfn, error))
    for lfn in sorted(res['Value']['Successful']):
      print("removed %s" % lfn)

  def do_addReplica(self, args):
    """ Add new replica to the transformation DB

    usage: addReplica <lfn> <se>
    """
    argss = args.split()
    if not len(argss) == 2:
      print("no file info supplied")
      return
    lfn = argss[0]
    se = argss[1]
    lfnDict = {}
    lfnDict[lfn] = {'PFN': 'IGNORED-PFN', 'SE': se, 'Size': 0, 'GUID': 'IGNORED-GUID', 'Checksum': 'IGNORED-CHECKSUM'}
    res = self.transClient.addReplica(lfnDict, force=True)
    if not res['OK']:
      print("failed to add replica: %s" % res['Message'])
      return
    for lfn in sorted(res['Value']['Failed']):
      error = res['Value']['Failed'][lfn]
      print("failed to add replica: %s" % (error))
    for lfn in sorted(res['Value']['Successful']):
      print("added %s" % lfn)

  def do_removeReplica(self, args):
    """Remove replica from the transformation DB

    usage: removeReplica <lfn> <se>
    """
    argss = args.split()
    if not len(argss) == 2:
      print("no file info supplied")
      return
    lfn = argss[0]
    se = argss[1]
    lfnDict = {}
    lfnDict[lfn] = {'PFN': 'IGNORED-PFN', 'SE': se, 'Size': 0, 'GUID': 'IGNORED-GUID', 'Checksum': 'IGNORED-CHECKSUM'}
    res = self.transClient.removeReplica(lfnDict)
    if not res['OK']:
      print("failed to remove replica: %s" % res['Message'])
      return
    for lfn in sorted(res['Value']['Failed']):
      error = res['Value']['Failed'][lfn]
      print("failed to remove replica: %s" % (error))
    for lfn in sorted(res['Value']['Successful']):
      print("removed %s" % lfn)

  def do_setReplicaStatus(self, args):
    """Set replica status, usually used to mark a replica Problematic

    usage: setReplicaStatus <lfn> <status> <se>
    """
    argss = args.split()
    if not len(argss) > 2:
      print("no file info supplied")
      return
    lfn = argss[0]
    status = argss[1]
    se = argss[2]
    lfnDict = {}
    lfnDict[lfn] = {
        'Status': status,
        'PFN': 'IGNORED-PFN',
        'SE': se,
        'Size': 0,
        'GUID': 'IGNORED-GUID',
        'Checksum': 'IGNORED-CHECKSUM'}
    res = self.transClient.setReplicaStatus(lfnDict)
    if not res['OK']:
      print("failed to set replica status: %s" % res['Message'])
      return
    for lfn in sorted(res['Value']['Failed']):
      error = res['Value']['Failed'][lfn]
      print("failed to set replica status: %s" % (error))
    for lfn in sorted(res['Value']['Successful']):
      print("updated replica status %s" % lfn)


if __name__ == "__main__":
  cli = TransformationCLI()
  cli.cmdloop()
