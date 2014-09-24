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
    
    records = list( records )

    nFields = len(fields)      
    if nFields != len(records[0]):
      out = "Incorrect data structure to print, nFields %d, nRecords %d" % ( nFields, len(records[0]) )
      if printOut:      
        print out
      return out

    if sortField:
      records.sort( None, lambda x: x[fields.index( sortField )] )

    lengths = []
    for i in range(nFields):
      lengths.append(len(fields[i]))
      for r in records:
        if len(r[i]) > lengths[i]:
          lengths[i] = len(r[i])

    
    numberWidth = len( str( len( records ) ) ) + 1      
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
      stringBuffer.write( fields[i].ljust(lengths[i]+separatorWidth) )
    stringBuffer.write( '\n' )
    stringBuffer.write( '='*totalLength + '\n' )
    count = 1
    for r in records:
      if numbering:
        if count == len(records) and records[-1][0] == "Total":
          stringBuffer.write( " "*(numberWidth+separatorWidth) )  
        else:  
          stringBuffer.write( str(count).rjust(numberWidth)+columnSeparator )
      
      for i in range(nFields):
        stringBuffer.write( r[i].ljust(lengths[i])+columnSeparator )
      
      stringBuffer.write( '\n' )    
      if count == len(records)-1 and records[-1][0] == "Total":
        stringBuffer.write( '-'*totalLength + '\n' )
      count += 1
      
    output = stringBuffer.getvalue()  
    if printOut:
      print output
      
    return output    