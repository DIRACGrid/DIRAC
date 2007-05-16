# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Protocols.py,v 1.7 2007/05/16 10:07:00 acasajus Exp $
__RCSID__ = "$Id: Protocols.py,v 1.7 2007/05/16 10:07:00 acasajus Exp $"

from DIRAC.Core.DISET.private.Transports import PlainTransport, SSLTransport

gProtocolDict = { 'dip' : ( PlainTransport.PlainTransport, PlainTransport.checkSanity ),
                  'dips' : ( SSLTransport.SSLTransport, SSLTransport.checkSanity )
                 }

gDefaultProtocol = 'dips'