import unittest,types,time
from DIRAC.Core.Utilities.Pfn import pfnparse,pfnunparse

class PfnTestCase(unittest.TestCase):

  def test_pfnparse(self):
    castorPfn = '/castor/cern.ch/grid/lhcb/production/somefile.file'
    res = pfnparse(castorPfn)
    pfnDict = res['Value']
    res = pfnunparse(pfnDict)
    newPfn = res['Value']
    self.assertEqual(castorPfn,newPfn)

    rfioPfn = 'rfio:/castor/cern.ch/grid/lhcb/production/somefile.file'
    res = pfnparse(rfioPfn)
    pfnDict = res['Value']
    res = pfnunparse(pfnDict)
    newPfn = res['Value']
    self.assertEqual(rfioPfn,newPfn)

    miniSrmPfn = 'srm://srm.cern.ch/castor/cern.ch/grid/lhcb/prodution/somefile.file'
    res = pfnparse(miniSrmPfn)
    pfnDict = res['Value']
    res = pfnunparse(pfnDict)
    newPfn = res['Value']
    self.assertEqual(miniSrmPfn,newPfn)

    portSrmPfn = 'srm://srm.cern.ch:8443/castor/cern.ch/grid/lhcb/prodution/somefile.file'
    res = pfnparse(portSrmPfn)
    pfnDict = res['Value']
    res = pfnunparse(pfnDict)
    newPfn = res['Value']
    self.assertEqual(portSrmPfn,newPfn)

    srmPfn = 'srm://srm.cern.ch:8443/srm/managerv2?SFN=/castor/cern.ch/grid/lhcb/prodution/somefile.file'
    res = pfnparse(srmPfn)
    pfnDict = res['Value']
    res = pfnunparse(pfnDict)
    newPfn = res['Value']
    self.assertEqual(srmPfn,newPfn)

    filePfn = 'file:/opt/dirac/somefile.file'
    res = pfnparse(filePfn)
    pfnDict = res['Value']
    res = pfnunparse(pfnDict)
    newPfn = res['Value']
    self.assertEqual(filePfn,newPfn)

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(PfnTestCase)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)


