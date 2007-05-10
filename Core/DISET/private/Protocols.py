# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Protocols.py,v 1.5 2007/05/10 18:44:59 acasajus Exp $
__RCSID__ = "$Id: Protocols.py,v 1.5 2007/05/10 18:44:59 acasajus Exp $"

from DIRAC.Core.DISET.private.Transports import PlainTransport, SSLTransport

gProtocolDict = { 'dirac' : PlainTransport.PlainTransport,
                  'diset' : SSLTransport.SSLTransport
                 }

gDefaultProtocol = 'dirac'