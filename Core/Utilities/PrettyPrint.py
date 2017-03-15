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
  while len( s ) > 0:
    news = s[-3:] + "," + news
    s = s[:-3]
  return news[:-1]

def printTable( fields, records, sortField = '', numbering = True,
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

  if not records:
    if printOut:
      print "No output"
    return "No output"

  nFields = len( fields )
  for rec in records:
    if nFields != len( rec ):
      out = "Incorrect data structure to print, nFields %d, nRecords %d" % ( nFields, len( rec ) )
      if printOut:
        print out
      return out

  # Strip all strings
  fieldList = [f.strip() for f in fields]
  recordList = [[r.strip() for r in rec] for rec in records]

  if sortField:
    recordList.sort( None, lambda x: x[fieldList.index( sortField )] )

  # Compute the maximum width for each field
  fieldWidths = []
  integerField = []
  for i in range( nFields ):
    l = max( len( fieldList[i] ), max( len( r[i] ) for r in recordList ) )
    fieldWidths.append( l )
    integerField.append( False not in set( r[i].isdigit() for r in recordList ) )

  numberWidth = len( str( len( recordList ) ) ) + 1
  separatorWidth = len( columnSeparator )
  totalLength = sum( fieldWidths ) + separatorWidth * nFields
  if numbering:
    totalLength += ( numberWidth + separatorWidth )

  stringBuffer = StringIO.StringIO()

  topLength = ( numberWidth + separatorWidth ) if numbering else 0
  stringBuffer.write( ' ' * ( topLength ) )
  for n, ( field, l ) in enumerate( zip( fieldList, fieldWidths ) ):
    # If the separator is ' ', no need to pas last field
    if n != ( nFields - 1 ) or columnSeparator != ' ':
      field = field.ljust( l + separatorWidth )
    stringBuffer.write( field )
    topLength += len( field )
  stringBuffer.write( '\n' + '=' * topLength + '\n' )

  for count, r in enumerate( recordList ):
    total = ( count == len( recordList ) - 1 and recordList[-1][0] == "Total" )
    if numbering:
      # Do not number the line with the total
      if total:
        stringBuffer.write( " "*( numberWidth + separatorWidth ) )
      else:
        stringBuffer.write( str( count ).rjust( numberWidth ) + columnSeparator )

    for n, ( field, l ) in enumerate( zip( r, fieldWidths ) ):
      if integerField[n]:
        # If the field is a set of integers, right justify, else left
        field = field.rjust( l ) + columnSeparator
      elif n != ( nFields - 1 ) or columnSeparator != ' ':
        # Last column doesn't require padding if sep is ' '
        field = field.ljust( l ) + columnSeparator
      stringBuffer.write( field )

    stringBuffer.write( '\n' )
    if total:
      stringBuffer.write( '-' * totalLength + '\n' )

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
    if isinstance( value, ( list, tuple ) ):
      line += ','.join( list( value ) )
    else:
      line += str( value )
    lines.append( line )
  return "{\n%s\n}" % "\n".join( lines )
