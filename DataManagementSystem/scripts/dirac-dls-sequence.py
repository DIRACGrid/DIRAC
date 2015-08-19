'''
Created on Jun 9, 2015

@author: Corentin Berger
'''

from DIRAC.Core.Base import Script

lfn = None
IDSeq = None
fullFlag = False
callerName = None
after = None
before = None
status = None
extra = None
group = None
userName = None
hostName = None

Script.registerSwitch( '', 'Full', 'Full print option' )
Script.registerSwitch( 'i:', 'ID=', 'ID of sequence ' )
Script.registerSwitch( 'n:', 'Name=', 'Name of caller ' )
Script.registerSwitch( 'a:', 'After=', 'Date, format be like 1999-12-31' )
Script.registerSwitch( 'b:', 'Before=', 'Date, format be like 1999-12-31' )
Script.registerSwitch( 'g:', 'Group=', 'A DIRAC Group' )
Script.registerSwitch( 'u:', 'UserName=', 'A DIRAC UserName' )
Script.registerSwitch( 'y:', 'HostName=', 'A HostName' )
Script.registerSwitch( 'z:', 'Status=', 'Failed, Successful or Unknown' )
Script.registerSwitch( 'e:', 'Extra=', 'A string, see below for more informations' )
Script.setUsageMessage( '\n'.join( [ __doc__,
                                    'WARNING : the maximum number of sequence to get from database is 500',
                                    'USAGE:',
                                    ' %s [OPTION|CFGFILE] LFN ' % Script.scriptName,
                                    'ARGUMENTS:',
                                    'For extra you have to pass first the name of the argument and after the value',
                                    'You can pass as many duo as you want like this :\n -e "JobID 17 Path /local/foo/bar"' ] ) )

Script.parseCommandLine( ignoreErrors = False )

for switch in Script.getUnprocessedSwitches():
  if switch[0] == "i" or switch[0].lower() == "id":
    IDSeq = switch[1]
  elif switch[0] == "n" or switch[0].lower() == "name":
    callerName = switch[1]
  elif switch[0] == "a" or switch[0].lower() == "after":
    after = switch[1]
  elif switch[0] == "b" or switch[0].lower() == "before":
    before = switch[1]
  elif switch[0] == "z" or switch[0].lower() == "status":
    status = switch[1]
  elif switch[0] == "e" or switch[0].lower() == "extra":
    extra = switch[1]
  elif switch[0] == "g" or switch[0].lower() == "Group":
    group = switch[1]
  elif switch[0] == "u" or switch[0].lower() == "UserName":
    userName = switch[1]
  elif switch[0] == "y" or switch[0].lower() == "HostName":
    hostName = switch[1]
  elif switch[0].lower() == "full":
    fullFlag = True
  else :
    extra.append( ( switch[0], switch[1] ) )

args = Script.getPositionalArgs()
if args :
  lfn = args[0]

from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient

def printSequence( seq, full = False ):
  seqLines = []
  line = 'Sequence %s, Caller %s %s %s %s ' % ( seq.sequenceID, seq.caller.name,
                                                ', UserName %s' % seq.userName.name if seq.userName else '',
                                                ', HostName %s' % seq.hostName.name if seq.hostName else '' ,
                                                ', Group %s' % seq.group.name if seq.group else '' )
  if seq.extra :
    line += 'Extra : '
    for key, value in seq.extra.items() :
      line += ' %s = %s ' % ( key, value )
  seqLines.append( line )
  stack = list()
  stack.append( [seq.methodCalls[0], 1] )
  while len( stack ) != 0 :
    el = stack.pop()
    mc = el[0]
    cpt = el[1]
    line = ''
    for x in range( cpt ):
      line += '\t'
    line += '%s, %s' % \
    ( mc.name.name, mc.creationTime )
    seqLines.append( line )
    for action in mc.actions :
      line = ''
      for x in range( cpt + 1 ):
        line += '\t'
      if full :
        line += '\t%s%s%s%s%s%s'\
          % ( action.status,
              ', File %s' % action.file.name if action.file else '',
              ', SourceSE %s' % action.srcSE.name if action.srcSE else '',
              ', TargetSE %s' % action.targetSE.name if action.targetSE else '',
              ', Extra %s' % action.extra if action.extra else '',
              ', ErrorMessage %s' % action.errorMessage if action.errorMessage else '' )
      else :
        line += '\t%s%s%s%s'\
            % ( action.status,
                ', File %s' % action.file.name if action.file else '',
                ', SourceSE %s' % action.srcSE.name if action.srcSE else '',
                ', TargetSE %s' % action.targetSE.name if action.targetSE else '' )
      seqLines.append( line )

    for child in reversed( mc.children ) :
      stack.append( [child, cpt + 1] )
  return '\n'.join( seqLines )


def printSequenceLFN( seq, lfn, full = False ):
  seqLines = []
  line = 'Sequence %s, Caller %s ' % ( seq.sequenceID, '%s' % seq.caller.name if seq.caller else 'None' )
  if seq.extra :
    line += ', Extra : '
    for key, value in seq.extra.items() :
      line += '%s = %s, ' % ( key, value )
  seqLines.append( line )
  cpt = 1
  stack = list()
  stack.append( [seq.methodCalls[0], 1] )
  while len( stack ) != 0 :
    el = stack.pop()
    mc = el[0]
    cpt = el[1]
    base = ''
    for x in range( cpt ):
      base += '\t'
    base += '%s, %s, ' % \
    ( mc.name.name, mc.creationTime )
    for action in mc.actions :

      if action.file.name == lfn:
        line = base
        if full :
          line += '%s%s%s%s%s '\
              % ( action.status,
                  ', SourceSE %s' % action.srcSE.name if action.srcSE else '',
                  ', TargetSE %s' % action.targetSE.name if action.targetSE else '',
                  ', Extra %s' % action.extra if action.extra else '',
                  ', ErrorMessage %s' % action.errorMessage if action.errorMessage else '' )
          seqLines.append( line )
        else :
          line += '%s%s%s'\
              % ( action.status,
                  ', SourceSE %s' % action.srcSE.name if action.srcSE else '',
                  ', TargetSE %s' % action.targetSE.name if action.targetSE else '' )
          seqLines.append( line )
    for child in mc.children :
      stack.append( [child, cpt + 1] )
  return '\n'.join( seqLines )



dlc = DataLoggingClient()
if lfn or callerName or after or before or status or extra or userName or hostName or group :
  res = dlc.getSequence( lfn, callerName, before, after, status, extra, userName, hostName, group )
  if res['OK']:
    if not res['Value'] :
      print 'no sequence to print'
    else :
      if lfn :
        for seq in res['Value'] :
          print printSequenceLFN( seq, lfn, full = fullFlag )
          print'\n'
      else :
        for seq in res['Value'] :
          print printSequence( seq, full = fullFlag )
          print'\n'
  else :
    print res['Message']

elif IDSeq :
  res = dlc.getSequenceByID( IDSeq )
  if res['OK']:
    if not res['Value'] :
      print 'no sequence to print'
    else :
      for seq in res['Value'] :
        print printSequence( seq, full = fullFlag )
        print'\n'
  else :
    print res['Message']
