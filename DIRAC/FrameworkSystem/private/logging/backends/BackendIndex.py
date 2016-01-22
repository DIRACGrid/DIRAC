
from DIRAC.FrameworkSystem.private.logging.backends.PrintBackend import PrintBackend
from DIRAC.FrameworkSystem.private.logging.backends.RemoteBackend import RemoteBackend
from DIRAC.FrameworkSystem.private.logging.backends.FileBackend import FileBackend
from DIRAC.FrameworkSystem.private.logging.backends.StdErrBackend import StdErrBackend

gBackendIndex = { 'stdout' : PrintBackend,
                  'stderr' : StdErrBackend,
                  'server' : RemoteBackend,
                  'file'   : FileBackend
                }
