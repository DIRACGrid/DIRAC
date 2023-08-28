from DIRAC.Core.DISET.private.Transports import PlainTransport, SSLTransport

gProtocolDict = {
    "dip": {
        "transport": PlainTransport.PlainTransport,
        "sanity": PlainTransport.checkSanity,
        "delegation": PlainTransport.delegate,
    },
    "dips": {
        "transport": SSLTransport.SSLTransport,
        "sanity": SSLTransport.checkSanity,
        "delegation": SSLTransport.delegate,
    },
}

gDefaultProtocol = "dips"
