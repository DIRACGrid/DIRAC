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
                printOut = True, columnSpace = 1 ):
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
    totalLength = 0
    for i in lengths:
      totalLength += i
      totalLength += columnSpace
    
    if numbering:        
      totalLength += numberWidth
          
    if numbering:      
      stringBuffer.write( ' '*(numberWidth+1) )      
    for i in range(nFields):
      stringBuffer.write( fields[i].ljust(lengths[i]+columnSpace) )
    stringBuffer.write( '\n' )
    stringBuffer.write( '='*totalLength + '\n' )
    count = 1
    for r in records:
      if numbering:
        if count == len(records) and records[-1][0] == "Total":
          stringBuffer.write( " "*(numberWidth+1) )  
        else:  
          stringBuffer.write( str(count).rjust(numberWidth)+' ' )
      
      for i in range(nFields):
        stringBuffer.write( r[i].ljust(lengths[i]+columnSpace) )
      
      stringBuffer.write( '\n' )    
      if count == len(records)-1 and records[-1][0] == "Total":
        stringBuffer.write( '-'*totalLength + '\n' )
      count += 1
      
    output = stringBuffer.getvalue()  
    if printOut:
      print output
      
    return output    