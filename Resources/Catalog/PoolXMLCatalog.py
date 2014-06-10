""" POOL XML Catalog Class
    This class handles simple XML-based File Catalog following the
    POOL project schema. It presents a DIRAC generic File Catalog interface
    although not complete and with several extensions
"""
__RCSID__ = "$Id$"

import os, xml.dom.minidom, types
from DIRAC import S_OK, S_ERROR

class PoolFile( object ):
  """
      A Pool XML File Catalog entry

      @author A.Tsaregorodtsev
  """
  def __init__( self, dom = None ):

    self.guid = ''
    self.pfns = []
    self.lfns = []

    if dom:
      self.guid = dom.getAttribute( 'ID' )
      physs = dom.getElementsByTagName( 'physical' )
      for p in physs:
        pfns = p.getElementsByTagName( 'pfn' )
        meta = p.getElementsByTagName( 'metadata' )
        for pfn in pfns:
          ftype = pfn.getAttribute( 'filetype' )
          name = pfn.getAttribute( 'name' )

          # Get the SE name if any
          se = "Uknown"
          for metadata in meta:
            mname = metadata.getAttribute( 'att_name' )
            if mname == name:
              se = metadata.getAttribute( 'att_value' )

          self.pfns.append( ( name, ftype, se ) )
      logics = dom.getElementsByTagName( 'logical' )
      for l in logics:
        # do not know yet the Pool lfn xml schema
        lfns = l.getElementsByTagName( 'lfn' )
        for lfn in lfns:
          name = lfn.getAttribute( 'name' )
        self.lfns.append( name )

  def dump( self ):
    """ Dumps the contents to the standard output
    """

    print "\nPool Catalog file entry:"
    print "   guid:", self.guid
    if len( self.lfns ) > 0:
      print "   lfns:"
      for l in self.lfns:
        print '     ', l
    if len( self.pfns ) > 0:
      print "   pfns:"
      for p in self.pfns:
        print '     ', p[0], 'type:', p[1], 'SE:', p[2]

  def getPfns( self ):
    """ Retrieves all the PFNs
    """
    result = []
    for p in self.pfns:
      result.append( ( p[0], p[2] ) )

    return result

  def getLfns( self ):
    """ Retrieves all the LFNs
    """
    result = []
    for l in self.lfns:
      result.append( l )

    return result

  def addLfn( self, lfn ):
    """ Adds one LFN
    """
    self.lfns.append( lfn )

  def addPfn( self, pfn, pfntype = None, se = None ):
    """ Adds one PFN
    """
    sename = "Unknown"
    if se: 
      sename = se

    if pfntype:
      self.pfns.append( ( pfn, pfntype, sename ) )
    else:
      self.pfns.append( ( pfn, 'ROOT_All', sename ) )

  def toXML( self, metadata ):
    """ Output the contents as an XML string
    """

    res = '\n  <File ID="' + self.guid + '">\n'
    if len( self.pfns ) > 0:
      res = res + '     <physical>\n'
      for p in self.pfns:
        #To properly escape <>& in POOL XML slice.
        fixedp = p[0].replace( "&", "&amp;" )
        fixedp = fixedp.replace( "&&amp;amp;", "&amp;" )
        fixedp = fixedp.replace( "<", "&lt" )
        fixedp = fixedp.replace( ">", "&gt" )
        res = res + '       <pfn filetype="' + p[1] + '" name="' + fixedp + '"/>\n'
      if metadata:
        for p in self.pfns:
          res = res + '       <metadata att_name="' + p[0] + '" att_value="' + p[2] + '"/>\n'
      res = res + '     </physical>\n'
    else:
      res = res + '     </physical>\n'

    if len( self.lfns ) > 0:
      res = res + '     <logical>\n'
      for l in self.lfns:
        res = res + '       <lfn name="' + l + '"/>\n'
      res = res + '     </logical>\n'
    else:
      res = res + '     </logical>\n'

    res = res + '   </File>\n'
    return res

class PoolXMLCatalog( object ):
  """ A Pool XML File Catalog
  """

  def __init__( self, xmlfile = '' ):
    """ PoolXMLCatalog constructor.

        Constructor takes one of the following argument types:
        xml string; list of xml strings; file name; list of file names
    """

    self.files = {}
    self.backend_file = None
    self.name = "Pool"

    # Get the dom representation of the catalog
    if xmlfile:
      if type( xmlfile ) == list:
        for xmlf in xmlfile:
          try:
            _sfile = file( xmlf, 'r' )
            self.dom = xml.dom.minidom.parse( xmlf )
          except:
            self.dom = xml.dom.minidom.parseString( xmlf )

          self.analyseCatalog( self.dom )
      else:
        try:
          _sfile = file( xmlfile, 'r' )
          self.dom = xml.dom.minidom.parse( xmlfile )
          # This is a file, set it as a backend by default
          self.backend_file = xmlfile
        except:
          self.dom = xml.dom.minidom.parseString( xmlfile )

        self.analyseCatalog( self.dom )

  def setBackend( self, fname ):
    """Set the backend file name

       Sets the name of the file which will receive the contents of the
       catalog when the flush() method will be called
    """

    self.backend_file = fname

  def flush( self ):
    """Flush the contents of the catalog to a file

       Flushes the contents of the catalog to a file from which
       the catalog was instanciated or which was set explicitely
       with setBackend() method
    """

    if os.path.exists( self.backend_file ):
      os.rename( self.backend_file, self.backend_file + '.bak' )

    bfile = open( self.backend_file, 'w' )
    print >> bfile, self.toXML()
    bfile.close()

  def getName( self ):
    """ Get the catalog type name
    """
    return S_OK( self.name )

  def analyseCatalog( self, dom ):
    """Create the catalog from a DOM object

       Creates the contents of the catalog from the DOM XML object
    """

    catalog = dom.getElementsByTagName( 'POOLFILECATALOG' )[0]
    pfiles = catalog.getElementsByTagName( 'File' )
    for p in pfiles:
      guid = p.getAttribute( 'ID' )
      pf = PoolFile( p )
      self.files[guid] = pf
      #print p.nodeName,guid

  def dump( self ):
    """Dump catalog

       Dumps the contents of the catalog to the std output
    """

    for _guid, pfile in self.files.items():
      pfile.dump()

  def getFileByGuid( self, guid ):
    """ Get PoolFile object by GUID
    """
    if guid in self.files.keys():
      return self.files[guid]
    else:
      return None

  def getGuidByPfn( self, pfn ):
    """ Get GUID for a given PFN
    """
    for guid, pfile in self.files.items():
      for p in pfile.pfns:
        if pfn == p[0]:
          return guid

    return ''

  def getGuidByLfn( self, lfn ):
    """ Get GUID for a given LFN
    """

    for guid, pfile in self.files.items():
      for l in pfile.lfns:
        if lfn == l:
          return guid

    return ''

  def getTypeByPfn( self, pfn ):
    """ Get Type for a given PFN
    """
    for _guid, pfile in self.files.items():
      for p in pfile.pfns:
        if pfn == p[0]:
          return p[1]

    return ''

  def exists( self, lfn ):
    """ Check for the given LFN existence
    """
    if self.getGuidByLfn( lfn ):
      return 1
    else:
      return 0

  def getLfnsList( self ):
    """Get list of LFNs in catalogue.
    """
    lfnsList = []
    for guid in self.files.keys():
      lfn = self.files[guid].getLfns()
      lfnsList.append( lfn[0] )

    return lfnsList

  def getLfnsByGuid( self, guid ):
    """ Get LFN for a given GUID
    """

    lfn = ''
    if guid in self.files.keys():
      lfns = self.files[guid].getLfns()
      lfn = lfns[0]

    if lfn:
      return S_OK( lfn )
    else:
      return S_ERROR( 'GUID ' + guid + ' not found in the catalog' )

  def getPfnsByGuid( self, guid ):
    """ Get replicas for a given GUID
    """

    result = S_OK()

    repdict = {}
    if guid in self.files.keys():
      pfns = self.files[guid].getPfns()
      for pfn, se in pfns:
        repdict[se] = pfn
    else:
      return S_ERROR( 'GUID ' + guid + ' not found in the catalog' )

    result['Replicas'] = repdict
    return result

  def getPfnsByLfn( self, lfn ):
    """ Get replicas for a given LFN
    """

    guid = self.getGuidByLfn( lfn )
    return self.getPfnsByGuid( guid )

  def removeFileByGuid( self, guid ):
    """ Remove file for a given GUID
    """

    for g, _pfile in self.files.items():
      if guid == g:
        del self.files[guid]

  def removeFileByLfn( self, lfn ):
    """ Remove file for a given LFN
    """

    for guid, pfile in self.files.items():
      for l in pfile.lfns:
        if lfn == l:
          if self.files.has_key( guid ):
            del self.files[guid]

  def addFile( self, fileTuple ):
    """ Add one or more files to the catalog
    """

    if type( fileTuple ) == types.TupleType:
      files = [fileTuple]
    elif type( fileTuple ) == types.ListType:
      files = fileTuple
    else:
      return S_ERROR( 'PoolXMLCatalog.addFile: Must supply a file tuple of list of tuples' )

    failed = {}
    successful = {}
    for lfn, pfn, se, guid, pfnType in files:
      #print '>'*10
      #print pfnType
      pf = PoolFile()
      pf.guid = guid
      if lfn:
        pf.addLfn( lfn )
      if pfn:
        pf.addPfn( pfn, pfnType, se )

      self.files[guid] = pf
      successful[lfn] = True

    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def addReplica( self, replicaTuple ):
    """ This adds a replica to the catalogue
        The tuple to be supplied is of the following form:
          (lfn,pfn,se,master)
        where master = True or False
    """
    if type( replicaTuple ) == types.TupleType:
      replicas = [replicaTuple]
    elif type( replicaTuple ) == types.ListType:
      replicas = replicaTuple
    else:
      return S_ERROR( 'PoolXMLCatalog.addReplica: Must supply a replica tuple of list of tuples' )

    failed = {}
    successful = {}
    for lfn, pfn, se, _master in replicas:
      guid = self.getGuidByLfn( lfn )
      if guid:
        self.files[guid].addPfn( pfn, None, se )
        successful[lfn] = True
      else:
        failed[lfn] = "LFN not found"

    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )


  def addPfnByGuid( self, lfn, guid, pfn, pfntype = None, se = None ):
    """ Add PFN for a given GUID - not standard
    """

    if guid in self.files.keys():
      self.files[guid].addPfn( pfn, pfntype, se )
    else:
      self.addFile( [ lfn, pfn, se, guid, pfntype ] )

  def addLfnByGuid( self, guid, lfn, pfn, se, pfntype ):
    """ Add LFN for a given GUID - not standard
    """

    if guid in self.files.keys():
      self.files[guid].addLfn( lfn )
    else:
      self.addFile( [ lfn, pfn, se, guid, pfntype ] )

  def toXML( self, metadata = False ):
    """ Convert the contents into an XML string
    """

    res = """<?xml version="1.0" encoding="UTF-8" standalone="no" ?>
<!-- Edited By PoolXMLCatalog.py -->
<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">
<POOLFILECATALOG>\n\n"""

    for _guid, pfile in self.files.items():
      res = res + pfile.toXML( metadata )

    res = res + "\n</POOLFILECATALOG>\n"
    return res
