#####################################################################
#
#  Utilities for pretty printing of table data
#  Author: A.Tsaregorodtsev
#
#####################################################################

__RCSID__ = '$Id$'

import StringIO

def int_with_commas(i):
  s = str(i)
  news = ''
  while len(s) > 0:
    news = s[-3:]+","+news
    s = s[:-3] 
  return news[:-1]

def printTable( fields, records, sortField='', numbering=True, 
                printOut = True, columnSeparator = ' ' ):
    """ Utility to pretty print tabular data
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
        stringBuffer.write( r[i].ljust( lengths[i] )+columnSeparator )
      
      stringBuffer.write( '\n' )    
      if count == len( recordList )-1 and recordList[-1][0] == "Total":
        stringBuffer.write( '-'*totalLength + '\n' )
      count += 1
      
    output = stringBuffer.getvalue()  
    if printOut:
      print output
      
    return output    