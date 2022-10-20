"""
  Provide uniform interface to backend for local and remote clients.return

  There's a pretty big assumption here: that DB and Handler expose the same calls, with identical signatures.
  This is not always the case.
"""


def getDBOrClient(DB, serverName):
    """Tries to instantiate the DB object and returns it if we manage to connect to the DB,
    otherwise returns a Client of the server
    """
    from DIRAC import gLogger
    from DIRAC.Core.Base.Client import Client

    try:
        database = DB()
        if database._connected:
            return database
    except Exception:
        pass

    gLogger.info(f"Can not connect to DB will use {serverName}")
    return Client(url=serverName)
