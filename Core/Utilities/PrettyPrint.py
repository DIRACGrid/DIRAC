#####################################################################
#
#  Utilities for pretty printing of table data
#  Author: A.Tsaregorodtsev
#
#####################################################################

__RCSID__ = '$Id$'

def int_with_commas(i):
  s = str(i)
  news = ''
  while len(s) > 0:
    news = s[-3:]+","+news
    s = s[:-3] 
  return news[:-1]

def printTable( fields, records, sortField='', numbering=True ):
    """ Utility to pretty print tabular data
    """

    if not records:
      print "No output"
      return
    
    records = list( records )

    nFields = len(fields)
    if nFields != len(records[0]):
      print "Incorrect data structure to print, nFields %d, nRecords %d" % ( nFields, len(records[0]) )
      return

    if sortField:
      records.sort( None, lambda x: x[fields.index( sortField )] )

    lengths = []
    for i in range(nFields):
      lengths.append(len(fields[i]))
      for r in records:
        if len(r[i]) > lengths[i]:
          lengths[i] = len(r[i])

    totalLength = 0
    for i in lengths:
      totalLength += i
      totalLength += 2
    totalLength += 2  
          
    if numbering:      
      print ' '*3,      
    for i in range(nFields):
      print fields[i].ljust(lengths[i]+1),
    print
    print '='*totalLength
    count = 1
    for r in records:
      if numbering:
        if count == len(records) and records[-1][0] == "Total":
          print " "*3,  
        else:  
          print str(count).rjust(3),
      
      for i in range(nFields):
        print r[i].ljust(lengths[i]+1),
      
      print    
      if count == len(records)-1 and records[-1][0] == "Total":
        print '-'*totalLength
      count += 1