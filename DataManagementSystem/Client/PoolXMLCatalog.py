########################################################################
# $Id: PoolXMLCatalog.py,v 1.1 2007/12/11 16:22:38 atsareg Exp $
########################################################################
""" POOL XML Catalog Class """

__RCSID__ = "$Id: PoolXMLCatalog.py,v 1.1 2007/12/11 16:22:38 atsareg Exp $"

import re, os, xml.dom.minidom
from DIRAC import S_OK, S_ERROR

class PoolFile:
  """ 
      A Pool XML File Catalog entry
      
      @author A.Tsaregorodtsev
  """
  def __init__(self,dom=None):
  
    self.guid = ''
    self.pfns = []
    self.lfns = []
    
    if dom:
      self.guid = dom.getAttribute('ID')
      physs = dom.getElementsByTagName('physical') 
      for p in physs:
        pfns = p.getElementsByTagName('pfn') 
        meta = p.getElementsByTagName('metadata') 
        for pfn in pfns:
          ftype = pfn.getAttribute('filetype')
          name = pfn.getAttribute('name')
          
          # Get the SE name if any
          se = "Uknown"
          for metadata in meta:
            mname = metadata.getAttribute('att_name')
            if mname == name:
              se = metadata.getAttribute('att_value')
              
          self.pfns.append((name,ftype,se))
      logics = dom.getElementsByTagName('logical')
      for l in logics:
        # do not know yet the Pool lfn xml schema
        lfns = l.getElementsByTagName('lfn') 
        for lfn in lfns:
          name = lfn.getAttribute('name')
        self.lfns.append(name)             
        
  def dump(self):
  
    print "\nPool Catalog file entry:"
    print "   guid:",self.guid
    if len(self.lfns)>0:
      print "   lfns:"
      for l in self.lfns:
        print '     ',l
    if len(self.pfns)>0:  
      print "   pfns:"
      for p in self.pfns:
        print '     ',p[0],'type:',p[1],'SE:',p[2]        
        
  def getPfns(self):
  
    result = []
    for p in self.pfns:
      result.append((p[0],p[2]))
          
    return result
    
  def getLfns(self):
    result = []
    for l in self.lfns:
      result.append(l)
          
    return result  
    
  def addLfn(self,lfn):
  
    self.lfns.append(lfn)
    
  def addPfn(self,pfn,pfntype = None,se=None):
  
    sename = "Unknown"
    if se: sename = se
  
    if pfntype:
      self.pfns.append((pfn,pfntype,sename)) 
    else:
      self.pfns.append((pfn,'ROOT_All',sename))   
      
  def toXML(self,metadata):
  
    res = '\n  <File ID="'+self.guid+'">\n'
    if len(self.pfns)>0:
      res = res + '     <physical>\n'
      for p in self.pfns:
        #To properly escape <>& in POOL XML slice.
        fixedp = p[0].replace("&","&amp;")
        fixedp = fixedp.replace("&&amp;amp;","&amp;")
        fixedp = fixedp.replace("<","&lt")        
        fixedp = fixedp.replace(">","&gt")
        res = res + '       <pfn filetype="'+p[1]+'" name="'+fixedp+'"/>\n'
      if metadata:  
        for p in self.pfns:
          res = res + '       <metadata att_name="'+p[0]+'" att_value="'+p[2]+'"/>\n'  
      res = res + '     </physical>\n'
    else:
      res = res + '     </physical>\n'  
      
    if len(self.lfns)>0:  
      res = res + '     <logical>\n'
      for l in self.lfns:
        res = res + '       <lfn name="'+l+'"/>\n'
      res = res + '     </logical>\n' 
    else:
      res = res + '     </logical>\n'     
      
    res = res + '   </File>\n'  
    return res    
    
class PoolXMLCatalog:
  """ 
      A Pool XML File Catalog 
      
      @author A.Tsaregorodtsev
  """
  
  def __init__(self,xmlfile=''):
    """ PoolXMLCatalog constructor. 
    
        Constructor takes one of the following argument types:
        xml string; list of xml strings; file name; list of file names
    """
  
    self.files = {}
    self.backend_file = None
    
    # Get the dom representation of the catalog
    if xmlfile:
      if type(xmlfile) == list:
        for xmlf in xmlfile:
          try:
            sfile = file(xmlf,'r')
            self.dom = xml.dom.minidom.parse(xmlf)  
          except:
            self.dom = xml.dom.minidom.parseString(xmlf)

          self.analyseCatalog(self.dom)
      else:
        try:
          sfile = file(xmlfile,'r')
          self.dom = xml.dom.minidom.parse(xmlfile)  
          # This is a file, set it as a backend by default
          self.backend_file = xmlfile
        except:
          self.dom = xml.dom.minidom.parseString(xmlfile)

        self.analyseCatalog(self.dom)
  
  def setBackend(self,fname):
    """Set the backend file name
    
       Sets the name of the file which will receive the contents of the
       catalog when the flush() method will be called
    """
  
    self.backend_file = fname
    
  def flush(self):
    """Flush the contents of the catalog to a file
    
       Flushes the contents of the catalog to a file from which
       the catalog was instanciated or which was set explicitely
       with setBackend() method
    """
  
    if os.path.exists(self.backend_file):
      os.rename(self.backend_file,self.backend_file+'.bak')
      
    bfile = open(self.backend_file,'w')
    print >>bfile,self.toXML()
    bfile.close()       
  
  def analyseCatalog(self,dom):
    """Create the catalog from a DOM object
    
       Creates the contents of the catalog from the DOM XML object
    """
    
    catalog = dom.getElementsByTagName('POOLFILECATALOG')[0]
    pfiles = catalog.getElementsByTagName('File')
    for p in pfiles:
      guid = p.getAttribute('ID')
      pf = PoolFile(p)
      self.files[guid] = pf
      #print p.nodeName,guid
      
  def dump(self):
    """Dump catalog
    
       Dumps the contents of the catalog to the std output
    """
  
    for guid,pfile in self.files.items():
      pfile.dump()  
      
  def getFileByGuid(self,guid):
  
    if guid in self.files.keys():
      return self.files[guid]
    else:
      return None
      
  def getGuidByPfn(self,pfn):
  
    for guid,pfile in self.files.items():
      for p in pfile.pfns:
        if pfn == p[0]:
          return guid  
          
    return '' 
    
  def getGuidByLfn(self,lfn):
  
    for guid,pfile in self.files.items():
      for l in pfile.lfns:
        if lfn == l:
          return guid  
          
    return ''  
    
  def exists(self,lfn):
  
    if self.getGuidByLfn(lfn):
      return 1
    else:
      return 0    
    
  def getLfnsByGuid(self,guid):
  
    result = S_OK()
  
    if guid in self.files.keys():
      lfns = self.files[guid].getLfns()
      for lfn in lfns:
      	result['Logical'] = lfn
    else:
      return S_ERROR('GUID '+guid+' not found in the catalog')
           
    return result  
      
  def getPfnsByGuid(self,guid):
  
    result = S_OK()
  
    repdict = {}
    if guid in self.files.keys():
      pfns = self.files[guid].getPfns()
      for pfn,se in pfns:
        repdict[se] = pfn
    else:
      return S_ERROR('GUID '+guid+' not found in the catalog')
      
    result['Replicas'] = repdict 
    return result  
      
  def getPfnsByLfn(self,lfn):   

    guid = self.getGuidByLfn(lfn)
    return self.getPfnsByGuid(guid) 
             
  def removeFileByGuid(self,guid):
  
    for g,pfile in self.files.items():
      if guid == g:
        del self.files[guid]  

  def removeFileByLfn(self,lfn):
  
    for guid,pfile in self.files.items():
      for l in pfile.lfns:
        if lfn == l:
          if self.files.has_key(guid): 
            del self.files[guid]  

  def addFile(self,guid,lfn=None,pfn=None,pfntype=None,se=None):
  
    pf = PoolFile()
    pf.guid = guid
    if lfn:
      pf.addLfn(lfn)
    if pfn:
      pf.addPfn(pfn,pfntype,se)
    self.files[guid]=pf
      
  def addPfnByGuid(self,guid,pfn,pfntype = None,se=None):
  
    if guid in self.files.keys():
      self.files[guid].addPfn(pfn,pfntype,se)        
    else:
      self.addFile(guid,pfn=pfn,pfntype=pfntype,se=se)
      
  def addLfnByGuid(self,guid,lfn):
  
    if guid in self.files.keys():
      self.files[guid].addLfn(lfn)        
    else:
      self.addFile(guid,lfn)      
      
  def toXML(self,metadata=False):
  
    res = """<?xml version="1.0" encoding="UTF-8" standalone="no" ?>
<!-- Edited By PoolXMLCatalog.py -->
<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">
<POOLFILECATALOG>\n\n"""

    for guid,pfile in self.files.items():   
      res = res + pfile.toXML(metadata)
      
    res = res + "\n</POOLFILECATALOG>"   
    return res      
