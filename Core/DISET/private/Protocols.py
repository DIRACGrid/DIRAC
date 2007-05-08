
from DIRAC.Core.DISET.private.Transports import PlainTransport, SSLTransport

gProtocolDict = { 'dit' : PlainTransport.PlainTransport,
                  'diset' : SSLTransport.SSLTransport
                 }

gDefaultProtocol = 'diset'