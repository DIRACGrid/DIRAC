
from DIRAC.LoggingSystem.private.backends.PrintBackend import PrintBackend
from DIRAC.LoggingSystem.private.backends.RemoteBackend import RemoteBackend

gBackendIndex = { 'stdout' : PrintBackend,
                  'server' : RemoteBackend
                }
