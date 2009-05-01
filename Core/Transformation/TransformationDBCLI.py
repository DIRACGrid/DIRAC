""" Transformation Database Client Command Line Interface. """

import re,time,types

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import shellCall

import cmd
import sys, os
import signal
import string
import time

def printDict(dictionary):
  """ Dictionary pretty printing
  """

  key_max = 0
  value_max = 0
  for key,value in dictionary.items():
    if len(key) > key_max:
      key_max = len(key)
    if len(str(value)) > value_max:
      value_max = len(str(value))
  for key,value in dictionary.items():
    print key.rjust(key_max),' : ',str(value).ljust(value_max)


class TransformationDBCLI(cmd.Cmd):

  def __init__( self ):
    cmd.Cmd.__init__( self )

  def setServer(self,oServer):
    self.server = oServer

  def printPair( self, key, value, separator=":" ):
    valueList = value.split( "\n" )
    print "%s%s%s %s" % ( key, " " * ( self.identSpace - len( key ) ), separator, valueList[0].strip() )
    for valueLine in valueList[ 1:-1 ]:
      print "%s  %s" % ( " " * self.identSpace, valueLine.strip() )

  def do_quit( self, *args ):
    """
    Exits the application
        Usage: quit
    """
    sys.exit( 0 )

  def do_help( self, args ):
    """ Default version of the help command
       Usage: help <command>
       OR use helpall to see description for all commans"""
    cmd.Cmd.do_help(self, args)

  # overriting default help command
  def do_helpall( self, args ):
    """
    Shows help information
        Usage: helpall <command>
        If no command is specified all commands are shown
    """
    if len( args ) == 0:
      print "\nAvailable commands:\n"
      attrList = dir( self )
      attrList.sort()
      for attribute in attrList:
        if attribute.find( "do_" ) == 0:
          self.printPair( attribute[ 3: ], getattr( self, attribute ).__doc__[ 1: ] )
          print ""
    else:
      command = args.split()[0].strip()
      try:
        obj = getattr( self, "do_%s" % command )
      except:
        print "There's no such %s command" % command
        return
      self.printPair( command, obj.__doc__[1:] )

  def check_params(self, args, num):
    """Checks if the number of parameters correct"""
    argss = string.split(args)
    length = len(argss)
    if length < num:
      print "Error: Number of arguments provided %d less that required %d, please correct." % (length, num)
      return (False, length)
    return (argss,length)

  def check_id_or_name(self, id_or_name):
      """resolve name or Id by converting type of argument """
      if id_or_name.isdigit():
          return long(id_or_name) # its look like id
      return id_or_name

# not in use
#  def convertIDtoString(self,id):
#    return ("%08d" % (id) )



  ####################################################################
  #
  # These are the methods to transformation manipulation
  #

  def do_getStatus(self,args):
    """Get transformation details

       usage: getStatus <transName>
    """
    argss = string.split(args)
    transName = argss[0]
    res = self.server.getTransformation(transName)
    if not res['OK']:
      print "Getting status of %s failed" % transName
    else:
      print "%s: %s" % (transName,res['Value'])
#
#  def do_setStatus(self,args):
#    """Set transformation status
#
#       usage: setStatus <transName> <Status>
#       Status <'Active' 'Stopped' 'New'>
#    """
#    argss = string.split(args)
#    transName = argss[0]
#    status = argss[1]
#    res = self.server.setTransformationStatus(transName,status)
#    if not res['OK']:
#      print "Setting status of %s to %s failed" % (transName,status)
#
  def do_start(self,args):
    """Start transformation

       usage: start <transID>
    """
    argss = string.split(args)
    transName = argss[0]
    transID = self.check_id_or_name(transName)
    res = self.server.setTransformationStatus(transID,'Active')
    if not res['OK']:
      print "Starting %s failed" % transName

  def do_stop(self,args):
    """Stop transformation

       usage: stop <transID>
    """
    argss = string.split(args)
    transName = argss[0]
    transID = self.check_id_or_name(transName)
    res = self.server.setTransformationStatus(transID,'Stopped')
    if not res['OK']:
      print "Stopping %s failed" % transName

  def do_flush(self,args):
    """Flush transformation

       usage: flush <transName>
    """
    argss = string.split(args)
    transName = argss[0]
    transID = self.check_id_or_name(transName)
    res = self.server.setTransformationStatus(transID,'Flush')
    if not res['OK']:
      print "Flushing %s failed" % transName
#
#  def do_get(self,args):
#    """Get transformation definition
#
#    usage: get <transName>
#    """
#    argss = string.split(args)
#    transName = argss[0]
#    res = self.server.getTransformationDefinition(transName)
#    if not res['OK']:
#      print "Failed to get %s" % transName
#    else:
#      istream = res['InputStreams']
#      print transName,istream.keys()[0],istream.values()[0]
#
#  def do_getStat(self,args):
#    """Get transformation statistics
#
#    usage: getStat <transName>
#    """
#    argss = string.split(args)
#    transName = argss[0]
#    res = self.server.getTransformationStats(transName)
#    if not res['OK']:
#      print "Failed to get statistics for %s" % transName
#    else:
#      print res['Value']
#
#  def do_modMask(self,args):
#    """Modify transformation input definition
#
#       usage: modInput <transName> <mask>
#    """
#    argss = string.split(args)
#    transName = argss[0]
#    mask = argss[1]
#    res = self.server.modifyTransformationInput(transName,mask)
#    if not res['OK']:
#      print "Failed to modify input stream for %s" % transName
#
#  def do_getall(self,args):
#    """Get transformation details
#
#       usage: getall
#    """
#    res = self.server.getAllTransformations()
#
  def do_shell(self,args):
    """Execute a shell command

       usage !<shell_command>
    """
    comm = args
    res = shellCall(0,comm)
    if res['OK'] and res['Value'][0] == 0:
      returnCode,stdOut,stdErr = res['Value']
      print "%s\n%s" % (stdOut,stdErr)
    else:
      print res['Message']
#
#  def do_exit(self,args):
#    """ Exit the shell.
#
#    usage: exit
#    """
#    sys.exit(0)
#
#  ####################################################################
#  #
#  # These are the methods to file manipulation
#  #
#
#  def do_addFiles(self,args):
#    """Add new file list
#
#    usage: addFiles <file_list_file> [force]
#    """
#    argss = string.split(args)
#    fname = argss[0]
#    force = 0
#    if len(argss) == 2:
#      if argss[1] == 'force':
#        force = 1
#
#    files = open(fname,'r')
#    lfns = []
#    for line in files.readlines():
#      if line.strip():
#        if not re.search("^#",line):
#          lfn = line.split()[1].strip()
#          se = line.split()[0].strip()
#          lfns.append((se,lfn))
#          print se,lfn
#
#    result = self.procdb.addFiles(lfns,force)
#    if result['Status'] != "OK":
#      print "Failed to add files",fname

#  def do_addPfn(self,args):
#    """Add new replica
#
#    usage: addPfn <lfn> <se>
#    """
#
#    argss = string.split(args)
#    lfn = argss[0]
#    se = argss[1]
#    result = self.procdb.addPfn(lfn,'',se)
#    if result['Status'] != "OK":
#      print "Failed to add replica",lfn,'on',se
#
#  def do_removeFile(self,args):
#    """Remove file specified by its LFN from the Processing DB
#
#    usage: removeFile <lfn>
#    """
#
#    argss = string.split(args)
#    lfn = argss[0]
#    result = self.procdb.rmFile(lfn)
#    if result['Status'] != "OK":
#      print "Failed to remove file",lfn
#

  def do_addDirectory(self,args):
    """Add files from the given catalog directory

    usage: addDirectory <directory> [force]
    """

    argss, length = self.check_params(args, 1)
    if not argss:
      return
    directory = argss[0]
    force = False
    if length > 1:
      if argss[1] == 'force':
        force = True

    # KGG checking if directory has / at the end, if yes we remove it
    directory=directory.rstrip('/')

    if not self.lfc:
      try:
        from DIRAC.DataManagementSystem.Client.Catalog.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
        self.lfc = LcgFileCatalogCombinedClient()
      except:
        self.lfc = None

    if self.lfc:
      start = time.time()
      result = self.lfc.getDirectoryReplicas(directory)
      end = time.time()
      print "getPfnsInDir",directory,"operation time",(end-start)

      lfns = []
      if result['OK']:
        if 'Successful' in result['Value'] and directory in result['Value']['Successful']:
          lfndict = result['Value']['Successful'][directory]

          for lfn,repdict in lfndict.items():
            for se,pfn in repdict.items():
              lfns.append((lfn,pfn,0,se,'IGNORED-GUID','IGNORED-CHECKSUM'))

          result = self.server.addFile(lfns, force)

          if not result['OK']:
            print "Failed to add files with local LFC interrogation"
            print "Trying the addDirectory on the Server side"
          else:
            print "Operation successfull"
            file_exists = 0
            forced = 0
            pass_filter = 0
            retained = 0
            replica_exists = 0
            added_to_calalog = 0
            added_to_transformation = 0
            total = len(result['Value']['Successful'])
            failed = len(result['Value']['Failed'])
            for fn in result['Value']['Successful']:
              f = result['Value']['Successful'][fn]
              if f['FileExists']:
                  file_exists = file_exists+1
              if f['Forced']:
                  forced = forced+1
              if f['PassFilter']:
                  pass_filter = pass_filter+1
              if f['Retained']:
                  retained = retained+1
              if f['ReplicaExists']:
                  replica_exists = replica_exists+1
              if f['AddedToCatalog']:
                  added_to_calalog = added_to_calalog+1
              if f['AddedToTransformation']:
                  added_to_transformation = added_to_transformation+1

            print 'Failed:',  failed
            print 'Successful:', total
            print 'Pass filters', pass_filter
            print 'Forced in:', forced
            print 'Pass filters + forced = Retained:', retained
            print 'Exists in Catalog', file_exists
            print 'Added to Catalog', added_to_calalog-file_exists
            print 'Added to Transformations', added_to_transformation
            print 'Replica Exists', replica_exists
            return
        else:
          print "No such directory in LFC"

    else:
      # Local file addition failed, try the remote one
      result = self.server.addDirectory(directory)
      print result
      if not result['OK']:
        print result['Message']
      else:
        print result['Value']

#
#  def do_setReplicaStatus(self,args):
#    """Set replica status, usually used to mark a replica Problematic
#
#    usage: setReplicaStatus <lfn> <status> [<site>]
#
#    The <site> can be ANY
#    """
#
#    argss = string.split(args)
#    lfn = argss[0]
#    status = argss[1]
#    if len(argss) > 2:
#      site = argss[2]
#    else:
#      site = "ANY"
#
#    try:
#      result = self.procdb.setReplicaStatus(lfn,status,site)
#      if result['Status'] == "OK":
#        print "Updated status for replica of",lfn,'at',site,'to',status
#      else:
#        print 'Failed to update status for replica of',lfn,'at',site,'to',status
#    except Exception, x:
#      print "setReplicaStatus failed: ", str(x)
#
#  def do_replicas(self,args):
#    """ Get replicas for <path>
#
#        usage: replicas <path>
#    """
#    path = string.split(args)[0]
#    #print "lfn:",path
#    try:
#      result =  self.catalog.getReplicaStatus(path)
#      if result['Status'] == 'OK':
#        ind = 0
#        for se,(entry,status) in result['ReplicaStatus'].items():
#          ind += 1
#          print ind,se.ljust(15),status.ljust(10)
#      else:
#        print "Replicas: ",result['Message']
#    except Exception, x:
#      print "replicas failed: ", str(x)
#

  def do_addFile(self,args):
    """ Add new file to the Production Database

    usage: addFile <lfn> <se> [force]
    """

    argss = string.split(args)
    lfn = argss[0]
    se = argss[1]
    force = False
    if len(argss) == 3:
      if argss[2] == 'force':
        force = True

    lfnTuple = (lfn,'',0,se,'IGNORED-GUID','IGNORED-CHECKSUM')

    result = self.server.addFile([lfnTuple],force)
    if not result['OK']:
      print "Failed to add file",lfn

    lfnDict = result['Value']['Successful']
    if lfnDict[lfn]['Retained']:
      print "File added"
    else:
      print "File not retained"

  def do_getFiles(self,args):
    """Get files for the given production

    usage: getFiles <production> [-j] [> <file_name>]

    Flags:

      -j  order output by job ID ( default order by file LFN )
    """

    argss = string.split(args)
    prod = argss[0]
    order_by_job = False
    if len(argss) >= 2:
      if argss[1] == '-j':
        order_by_job = True

    ofname = ''
    if len(argss) >= 2:
      if argss[-2] == '>':
        ofname = argss[-1]
        ofile = open(ofname,'w')

    result = self.server.getFilesForTransformation(int(prod),order_by_job)
    if result['OK']:
      files = result['Value']
      if files:
        #print "          LFN                                         ", \
        #      "Status        Stream     JobID                  Used SE"
        for f in files:
          lfn = f['LFN']
          status = f['Status']
          jobid = f['JobID']
          if jobid.find('No JobID assigned') == -1:
            if jobid.find('_') == -1:
              jobname = str(int(prod)).zfill(8)+'_'+str(jobid).zfill(8)
            else:
              jobname = str(jobid)
          else:
            jobname = str(jobid)
          usedse = f['TargetSE']
          if ofname:
            ofile.write(lfn.ljust(50)+' '+status.rjust(10)+' '+ \
                        jobname.rjust(19)+' '+usedse.rjust(16)+'\n')
          else:
            print lfn.ljust(50),status.rjust(10), \
                jobname.rjust(19),usedse.rjust(16)

    if ofname:
      print "Output is written to file",ofname
      ofile.close()

  def do_getFile(self,args):
    """Get files for the given production

    usage: getFile <lfn> [<production>]

    """

    argss = string.split(args)
    lfn = argss[0]
    prod = ''
    if len(argss) == 2:
      prod = int(argss[1])

    result = self.server.getFileSummary([lfn],prod)

    if not result['OK']:
      print "Command failed with message:",result['Message']
      return

    if result['Value']['Failed'] or not result['Value']['Successful'].has_key(lfn):
      print "Failed: lfn not found"
      return

    resDict = result['Value']['Successful'][lfn]
    print "\nStatus for %s:\n" % lfn
    print "Transformation           FileStatus                 Job                  TargetSE       UsedSE       LastUpdate"
    for transID,lfnDict in resDict.items():
      jobid = lfnDict['JobID']
      if jobid.find('No JobID assigned') == -1:
        if jobid.find('_') == -1:
          jobname = str(int(transID)).zfill(8)+'_'+str(jobid).zfill(8)
        else:
          jobname = str(jobid)
      else:
        jobname = str(jobid)
      fstatus = lfnDict['FileStatus']
      tranString = str(transID)+'/'+lfnDict['TransformationStatus']
      jobStatus = lfnDict['JobStatus']
      jobString = jobname+'/'+jobStatus
      se = lfnDict['TargetSE']
      usedSE = lfnDict['UsedSE']
      lastUpdate = lfnDict['LastUpdate']
      print tranString.ljust(16), fstatus.rjust(18),' '*4,jobString.ljust(27),se.rjust(11),usedSE.rjust(16),"  ",lastUpdate

  def do_setFileStatus(self,args):
    """Set file status for the given production

    usage: setFileStatus <production> <lfn> <status>
    """

    argss = string.split(args)
    if len(argss) != 3:
      print "usage: setFileStatus <production> <lfn> <status>"
      return

    prod = argss[0]
    lfn = argss[1]
    status = str(argss[2])

    result = self.server.setFileStatusForTransformation(int(prod),[(status,[lfn])])

    if not result['OK']:
      print "Failed to update status for file",lfn
    for lfn,message in result['Value']['Successful'].items():
      print "Successful:",lfn,":",message
    for lfn,message in result['Value']['Failed'].items():
      print "Failed:",lfn,":",message

  def do_resetFile(self,args):
    """Set file status for the given production

    usage: setFileStatus <production> <lfn> <status>
    """

    argss = string.split(args)
    if len(argss) != 2:
      print "usage: resetFile <production> <lfn>"
      return

    prod = argss[0]
    lfn = argss[1]

    result = self.server.resetFileStatusForTransformation(int(prod),[lfn])

    if not result['OK']:
      print "Failed to reset status for file",lfn

  def do_createBkQuery(self,args):
    """ Create a new Bookkeeping Query to be used in production definitions.

        Usage: createBkQuery [queryID]

        - queryID - ID of a Bookkeeping Query to get initial parameters from
    """

    fields = ['SimulationConditions',
              'DataTakingConditions',
              'ProcessingPass',
              'FileType',
              'EventType',
              'ConfigName',
              'ConfigVersion',
              'ProductionID',
              'DataQualityFlag']

    resultQuery = {'SimulationConditions':'All',
                'DataTakingConditions':'All',
                'ProcessingPass':'No default!',
                'FileType':'DST',
                'EventType':90000000,
                'ConfigName':'All',
                'ConfigVersion':'All',
                'ProductionID':0,
                'DataQualityFlag':'OK'}

    argss, length = self.check_params(args, 0)
    if length:
      queryID = int(argss[0])
      result = self.server.getBookkeepingQuery(queryID)
      if not result['OK']:
        print "Failed to get initial query:",result['Message']
        return

      resultQuery = result['Value']
      if resultQuery.has_key('BkQueryID'):
        del resultQuery['BkQueryID']

    done = False
    while not done:
      OK = True
      for field in fields:
        value = raw_input('%s [%s]: ' % (field,resultQuery[field]) )
        if value:
          resultQuery[field] = value

      print "\nResulting query:"
      printDict(resultQuery)
      print

      # Do some basic verification of the values
      if resultQuery['SimulationConditions'] != "All" and resultQuery['DataTakingConditions'] != "All":
        print "SimulationConditions and DataTakingConditions can not be defined simultaneously !"
        OK = False
#      if resultQuery['SimulationConditions'] == "All" and resultQuery['DataTakingConditions'] == "All":
#        print "Either SimulationConditions or DataTakingConditions must be defined !"
#        OK = False
      if resultQuery['ProcessingPass'] == "No default!":
        print "ProcessingPass must be defined !"
        OK = False
      for par in ['EventType','ProductionID']:
        try:
          dummy = int(resultQuery[par])
          resultQuery[par] = dummy
        except:
          print "%s must be integer" % par
          OK = False

      if OK:
        value = raw_input("OK,[A]bort,[R]etry:  [OK]:")
      else:
        value = raw_input("[A]bort,[R]etry:  [R]:")
        if not value:
          value = 'r'

      if not value and OK:
        result = self.server.addBookkeepingQuery(resultQuery)
        if not result['OK']:
          print "Query definition failed: ",result['Message']
        else:
          print "Query ID: ",result['Value']
        done = True
      elif value.lower() == 'a':
        done = True
      elif value.lower() == 'r':
        pass
      else:
        done = True

  def do_deleteBkQuery(self,args):
    """ Delete an existing Bookkeeping Query

        Usage: deleteBkQuery queryID

        - queryID - ID of a Bookkeeping Query to be deleted
    """

    argss, length = self.check_params(args, 1)
    queryID = int(argss[0])

    result = self.server.deleteBookkeepingQuery(queryID)
    if not result['OK']:
      print "Failed to delete Bookkeeping Query:",result['Message']

  def do_listBkQueries(self,args):
    """ Show existing Bookkeeping queries in the system

        Usage: listBkQueries
    """

    fields = ['BkQueryID',
              'SimulationConditions',
              'DataTakingConditions',
              'ProcessingPass',
              'FileType',
              'EventType',
              'ConfigName',
              'ConfigVersion',
              'ProductionID',
              'DataQualityFlag']

    widths = {'BkQueryID':9,
              'SimulationConditions':30,
              'DataTakingConditions':20,
              'ProcessingPass':20,
              'FileType':8,
              'EventType':10,
              'ConfigName':15,
              'ConfigVersion':15,
              'ProductionID':12,
              'DataQualityFlag':15}

    result = self.server.getBookkeepingQuery(0)
    if not result['OK']:
      print "Failed to get response from the service:",result['Message']
      return

    keys = [ string.ljust(string.rjust(x,widths[x]-(widths[x]-len(x))/2),widths[x]) for x in fields ]
    print 164*'-'
    print '|'.join(keys)+"|"
    print 164*'-'

    bkDict = result['Value']
    queryIDList = bkDict.keys()
    queryIDList.sort()
    for queryID in queryIDList:
      values = ''
      for field in fields:
        width = widths[field]
        value = str(bkDict[queryID][field])
        values += string.ljust(string.rjust(value,width-(width-len(value))/2),width) + '|'
      print values
    print 164*'-'

if __name__ == "__main__":

    import DIRAC
    cli = TransformationDBCLI()
    cli.cmdloop()
