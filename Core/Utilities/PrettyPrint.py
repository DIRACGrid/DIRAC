#####################################################################
#
#  Utilities for pretty printing table data and more
#  Author: A.Tsaregorodtsev
#
#####################################################################

__RCSID__ = '$Id$'

import StringIO

def int_with_commas( inputValue ):
  """ Utility to make a string of a large integer with comma separated degrees
      of thousand

  :param int inputValue: value to be interpreted
  :return: output string
  """
  s = str( inputValue )
  news = ''
  while len(s) > 0:
    news = s[-3:]+","+news
    s = s[:-3] 
  return news[:-1]

def printTable( fields, records, sortField='', numbering=True, 
                printOut = True, columnSeparator = ' ' ):
  """ Utility to pretty print tabular data

  :param list fields: list of column names
  :param list records: list of records, each record is a list or tuple of string values
  :param str sortField: name of the column by which the output will be sorted
  :param bool numbering: flag for numbering rows
  :param bool printOut: flag for printing into the stdout
  :param str columnSeparator: string to be used as a column separator
  :return: pretty table string
  """

  stringBuffer = StringIO.StringIO()

  if not records:
    if printOut:
      print "No output"
    return "No output"

  fieldList = list( fields )
  recordList = []
  for r in records:
    recordList.append( list( r ) )


  nFields = len( fieldList )
  if nFields != len( recordList[0] ):
    out = "Incorrect data structure to print, nFields %d, nRecords %d" % ( nFields, len( recordList[0] ) )
    if printOut:
      print out
    return out

  if sortField:
    recordList.sort( None, lambda x: x[fieldList.index( sortField )] )

  lengths = []
  for i in range(nFields):
    fieldList[i] = fieldList[i].strip()
    lengths.append( len( fieldList[i] ) )
    for r in recordList:
      r[i] = r[i].strip()
      if len( r[i] ) > lengths[i]:
        lengths[i] = len( r[i] )


  numberWidth = len( str( len( recordList ) ) ) + 1
  separatorWidth = len( columnSeparator )
  totalLength = 0
  for i in lengths:
    totalLength += i
    totalLength += separatorWidth

  if numbering:
    totalLength += (numberWidth + separatorWidth)

  if numbering:
    stringBuffer.write( ' '*(numberWidth+separatorWidth) )
  for i in range(nFields):
    stringBuffer.write( fieldList[i].ljust(lengths[i]+separatorWidth) )
  stringBuffer.write( '\n' )
  stringBuffer.write( '='*totalLength + '\n' )
  count = 1
  for r in recordList:
    if numbering:
      if count == len( recordList ) and recordList[-1][0] == "Total":
        stringBuffer.write( " "*(numberWidth+separatorWidth) )
      else:
        stringBuffer.write( str(count).rjust(numberWidth)+columnSeparator )

    for i in range( nFields ):
      #try casting to int and then align to the right, if it fails align to the left
      try:
        _val = int( "".join( r[i].split(",") ) )
        stringBuffer.write( r[i].rjust( lengths[i] )+columnSeparator )
      except ValueError:
        stringBuffer.write( r[i].ljust( lengths[i] )+columnSeparator )

    stringBuffer.write( '\n' )
    if count == len( recordList )-1 and recordList[-1][0] == "Total":
      stringBuffer.write( '-'*totalLength + '\n' )
    count += 1

  output = stringBuffer.getvalue()
  if printOut:
    print output

  return output

def printDict( dDict, printOut = False ):
  """ Utility to pretty print a dictionary

  :param dict dDict: Dictionary to be printed out
  :param bool printOut: flag to print to the stdout
  :return: pretty dictionary string
  """
  lines = []
  keyLength = 0
  for key in dDict:
    if len( key ) > keyLength:
      keyLength = len( key )
  for key in sorted( dDict ):
    line = "%s: " % key
    line = line.ljust( keyLength + 2 )
    value = dDict[ key ]
    if isinstance( value, (list, tuple) ):
      line += ','.join( list( value ) )
    else:
      line += str( value )
    lines.append( line )
  return "{\n%s\n}" % "\n".join( lines )