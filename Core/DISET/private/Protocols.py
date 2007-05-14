# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Protocols.py,v 1.6 2007/05/14 16:02:43 acasajus Exp $
__RCSID__ = "$Id: Protocols.py,v 1.6 2007/05/14 16:02:43 acasajus Exp $"

from DIRAC.Core.DISET.private.Transports import PlainTransport, SSLTransport

gProtocolDict = { 'dip' : PlainTransport.PlainTransport,
                  'dips' : SSLTransport.SSLTransport
                 }

gDefaultProtocol = 'dips'