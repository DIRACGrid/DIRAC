""" POOL XML Catalog Class
    This class handles simple XML-based File Catalog following the
    POOL project schema. It presents a DIRAC generic File Catalog interface
    although not complete and with several extensions
"""
import os
import xml.dom.minidom
from DIRAC import S_OK, S_ERROR


class PoolFile:
    """
    A Pool XML File Catalog entry

    @author A.Tsaregorodtsev
    """

    def __init__(self, dom=None):

        self.guid = ""
        self.pfns = []
        self.lfns = []

        if dom:
            self.guid = dom.getAttribute("ID")
            physs = dom.getElementsByTagName("physical")
            for p in physs:
                pfns = p.getElementsByTagName("pfn")
                meta = p.getElementsByTagName("metadata")
                for pfn in pfns:
                    ftype = pfn.getAttribute("filetype")
                    name = pfn.getAttribute("name")
                    # Get the SE name if any
                    se = pfn.getAttribute("se")
                    se = se if se else "Unknown"

                    self.pfns.append((name, ftype, se))
            logics = dom.getElementsByTagName("logical")
            for l in logics:
                # do not know yet the Pool lfn xml schema
                lfns = l.getElementsByTagName("lfn")
                for lfn in lfns:
                    name = lfn.getAttribute("name")
                self.lfns.append(name)

    def dump(self):
        """Dumps the contents to the standard output"""

        print("\nPool Catalog file entry:")
        print("   guid:", self.guid)
        if len(self.lfns) > 0:
            print("   lfns:")
            for l in self.lfns:
                print("     ", l)
        if len(self.pfns) > 0:
            print("   pfns:")
            for p in self.pfns:
                print("     ", p[0], "type:", p[1], "SE:", p[2])

    def getPfns(self):
        """Retrieves all the PFNs"""
        result = []
        for p in self.pfns:
            result.append((p[0], p[2]))

        return result

    def getLfns(self):
        """Retrieves all the LFNs"""
        result = []
        for l in self.lfns:
            result.append(l)

        return result

    def addLfn(self, lfn):
        """Adds one LFN"""
        self.lfns.append(lfn)

    def addPfn(self, pfn, pfntype=None, se=None):
        """Adds one PFN"""
        sename = "Unknown"
        if se:
            sename = se

        if pfntype:
            self.pfns.append((pfn, pfntype, sename))
        else:
            self.pfns.append((pfn, "ROOT_All", sename))

    def toXML(self, metadata):
        """Output the contents as an XML string"""

        doc = xml.dom.minidom.Document()

        fileElt = doc.createElement("File")
        fileElt.setAttribute("ID", self.guid)
        if self.pfns:
            physicalElt = doc.createElement("physical")
            fileElt.appendChild(physicalElt)
            for p in self.pfns:
                pfnElt = doc.createElement("pfn")
                physicalElt.appendChild(pfnElt)

                # To properly escape <>& in POOL XML slice.
                fixedp = p[0].replace("&", "&amp;")
                fixedp = fixedp.replace("&&amp;amp;", "&amp;")
                fixedp = fixedp.replace("<", "&lt")
                fixedp = fixedp.replace(">", "&gt")

                pfnElt.setAttribute("filetype", p[1])
                pfnElt.setAttribute("name", fixedp)
                pfnElt.setAttribute("se", p[2])

            if metadata:
                for p in self.pfns:
                    metadataElt = doc.createElement("metadata")
                    physicalElt.appendChild(metadataElt)

                    metadataElt.setAttribute("att_name", p[0])
                    metadataElt.setAttribute("att_value", p[2])

        if self.lfns:
            logicalElt = doc.createElement("logical")
            fileElt.appendChild(logicalElt)
            for l in self.lfns:
                lfnElt = doc.createElement("lfn")
                logicalElt.appendChild(lfnElt)

                lfnElt.setAttribute("name", l)
        return fileElt.toprettyxml(indent="   ")


class PoolXMLCatalog:
    """A Pool XML File Catalog"""

    def __init__(self, xmlfile=""):
        """PoolXMLCatalog constructor.

        Constructor takes one of the following argument types:
        xml string; list of xml strings; file name; list of file names
        """

        self.files = {}
        self.backend_file = None
        self.name = "Pool"

        # Get the dom representation of the catalog
        if xmlfile:
            if not isinstance(xmlfile, list):
                if os.path.isfile(xmlfile):
                    self.backend_file = xmlfile
                xmlfile = [xmlfile]

            for xmlf in xmlfile:
                if os.path.isfile(xmlf):
                    self.dom = xml.dom.minidom.parse(xmlf)
                else:
                    self.dom = xml.dom.minidom.parseString(xmlf)
                self.analyseCatalog(self.dom)

    def setBackend(self, fname):
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
            os.rename(self.backend_file, self.backend_file + ".bak")
        with open(self.backend_file, "w") as fp:
            fp.write(self.toXML())

    def getName(self):
        """Get the catalog type name"""
        return S_OK(self.name)

    def analyseCatalog(self, dom):
        """Create the catalog from a DOM object

        Creates the contents of the catalog from the DOM XML object
        """

        catalog = dom.getElementsByTagName("POOLFILECATALOG")[0]
        pfiles = catalog.getElementsByTagName("File")
        for p in pfiles:
            guid = p.getAttribute("ID")
            pf = PoolFile(p)
            self.files[guid] = pf
            # print p.nodeName,guid

    def dump(self):
        """Dump catalog

        Dumps the contents of the catalog to the std output
        """

        for _guid, pfile in self.files.items():
            pfile.dump()

    def getFileByGuid(self, guid):
        """Get PoolFile object by GUID"""
        if guid in self.files:
            return self.files[guid]
        return None

    def getGuidByPfn(self, pfn):
        """Get GUID for a given PFN"""
        for guid, pfile in self.files.items():
            for p in pfile.pfns:
                if pfn == p[0]:
                    return guid

        return ""

    def getGuidByLfn(self, lfn):
        """Get GUID for a given LFN"""

        for guid, pfile in self.files.items():
            for l in pfile.lfns:
                if lfn == l:
                    return guid

        return ""

    def getTypeByPfn(self, pfn):
        """Get Type for a given PFN"""
        for _guid, pfile in self.files.items():
            for p in pfile.pfns:
                if pfn == p[0]:
                    return p[1]

        return ""

    def exists(self, lfn):
        """Check for the given LFN existence"""
        if self.getGuidByLfn(lfn):
            return 1
        return 0

    def getLfnsList(self):
        """Get list of LFNs in catalogue."""
        lfnsList = []
        for guid in self.files:
            lfn = self.files[guid].getLfns()
            lfnsList.append(lfn[0])

        return lfnsList

    def getLfnsByGuid(self, guid):
        """Get LFN for a given GUID"""

        lfn = ""
        if guid in self.files:
            lfns = self.files[guid].getLfns()
            lfn = lfns[0]

        if lfn:
            return S_OK(lfn)
        else:
            return S_ERROR("GUID " + guid + " not found in the catalog")

    def getPfnsByGuid(self, guid):
        """Get replicas for a given GUID"""

        result = S_OK()

        repdict = {}
        if guid in self.files:
            pfns = self.files[guid].getPfns()
            for pfn, se in pfns:
                repdict[se] = pfn
        else:
            return S_ERROR("GUID " + guid + " not found in the catalog")

        result["Replicas"] = repdict
        return result

    def getPfnsByLfn(self, lfn):
        """Get replicas for a given LFN"""

        guid = self.getGuidByLfn(lfn)
        return self.getPfnsByGuid(guid)

    def removeFileByGuid(self, guid):
        """Remove file for a given GUID"""

        for g, _pfile in self.files.items():
            if guid == g:
                del self.files[guid]

    def removeFileByLfn(self, lfn):
        """Remove file for a given LFN"""

        for guid, pfile in self.files.items():
            for l in pfile.lfns:
                if lfn == l:
                    if guid in self.files:
                        del self.files[guid]

    def addFile(self, fileTuple):
        """Add one or more files to the catalog"""

        if isinstance(fileTuple, tuple):
            files = [fileTuple]
        elif isinstance(fileTuple, list):
            files = fileTuple
        else:
            return S_ERROR("PoolXMLCatalog.addFile: Must supply a file tuple of list of tuples")

        failed = {}
        successful = {}
        for lfn, pfn, se, guid, pfnType in files:
            # print '>'*10
            # print pfnType
            pf = PoolFile()
            pf.guid = guid
            if lfn:
                pf.addLfn(lfn)
            if pfn:
                pf.addPfn(pfn, pfnType, se)

            self.files[guid] = pf
            successful[lfn] = True

        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def addReplica(self, replicaTuple):
        """This adds a replica to the catalogue
        The tuple to be supplied is of the following form:

          (lfn,pfn,se,master)

        where master = True or False
        """
        if isinstance(replicaTuple, tuple):
            replicas = [replicaTuple]
        elif isinstance(replicaTuple, list):
            replicas = replicaTuple
        else:
            return S_ERROR("PoolXMLCatalog.addReplica: Must supply a replica tuple of list of tuples")

        failed = {}
        successful = {}
        for lfn, pfn, se, _master in replicas:
            guid = self.getGuidByLfn(lfn)
            if guid:
                self.files[guid].addPfn(pfn, None, se)
                successful[lfn] = True
            else:
                failed[lfn] = "LFN not found"

        resDict = {"Failed": failed, "Successful": successful}
        return S_OK(resDict)

    def addPfnByGuid(self, lfn, guid, pfn, pfntype=None, se=None):
        """Add PFN for a given GUID - not standard"""

        if guid in self.files:
            self.files[guid].addPfn(pfn, pfntype, se)
        else:
            self.addFile([lfn, pfn, se, guid, pfntype])

    def addLfnByGuid(self, guid, lfn, pfn, se, pfntype):
        """Add LFN for a given GUID - not standard"""

        if guid in self.files:
            self.files[guid].addLfn(lfn)
        else:
            self.addFile([lfn, pfn, se, guid, pfntype])

    def toXML(self, metadata=False):
        """Convert the contents into an XML string"""

        res = """<?xml version="1.0" encoding="UTF-8" standalone="no" ?>
<!-- Edited By PoolXMLCatalog.py -->
<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">
<POOLFILECATALOG>\n\n"""

        for _guid, pfile in self.files.items():
            res = res + pfile.toXML(metadata)

        res = res + "\n</POOLFILECATALOG>\n"
        return res
