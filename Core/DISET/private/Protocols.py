
from DIRAC.Core.DISET.private.Transports import PlainTransport, SSLTransport

gProtocolDict = { 'dirac' : PlainTransport.PlainTransport,
                  'diset' : SSLTransport.SSLTransport
                 }

gDefaultProtocol = 'dirac'