# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/DISET/private/Protocols.py,v 1.8 2008/06/05 10:20:16 acasajus Exp $
__RCSID__ = "$Id: Protocols.py,v 1.8 2008/06/05 10:20:16 acasajus Exp $"

from DIRAC.Core.DISET.private.Transports import PlainTransport, SSLTransport

gProtocolDict = { 'dip'  : { 'transport'  : PlainTransport.PlainTransport,
                             'sanity'     : PlainTransport.checkSanity,
                             'delegation' : PlainTransport.delegate
                           },
                  'dips' : { 'transport'  : SSLTransport.SSLTransport,
                             'sanity'     : SSLTransport.checkSanity,
                             'delegation' : SSLTransport.delegate
                           }
                 }

gDefaultProtocol = 'dips'