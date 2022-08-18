########################################################################
# File :    HttpStorageAccessHandler.py
# Author :  A.T.
########################################################################

"""  The HttpStorageAccessHandler is a http server request handler to provide a secure http
     access to the DIRAC StorageElement and StorageElementProxy. It is derived from the
     SimpleHTTPRequestHandler standard python handler
"""
import os
import shutil
import random

from http import server

from DIRAC.Core.Utilities.DictCache import DictCache


class HttpStorageAccessHandler(server.BaseHTTPRequestHandler):

    register = DictCache()
    basePath = ""

    def do_GET(self):
        """Serve a GET request."""

        # Strip off leading slash
        key = self.path[1:]
        if not self.register.exists(key):
            self.send_error(401, "Invalid key provided, access denied")
            return None

        cache_path = self.register.get(key)
        fileList = os.listdir(cache_path)
        if len(fileList) == 1:
            path = os.path.join(cache_path, fileList[0])
        else:
            # multiple files, make archive
            unique = str(random.getrandbits(24))
            fileString = " ".join(fileList)
            os.system(f"tar -cf {cache_path}/dirac_data_{unique}.tar --remove-files -C {cache_path} {fileString}")
            path = os.path.join(cache_path, "dirac_data_%s.tar" % unique)

        f = self.send_head(path)
        if f:
            shutil.copyfileobj(f, self.wfile)
            f.close()
            self.register.delete(key)

    def send_head(self, path):
        """Prepare headers for the file download"""
        # path = self.translate_path(self.path)
        f = None
        try:
            # Always read in binary mode. Opening files in text mode may cause
            # newline translations, making the actual size of the content
            # transmitted *less* than the content-length!
            f = open(path, "rb")
        except OSError:
            self.send_error(404, "File not found")
            return None
        self.send_response(200)
        self.send_header("Content-type", "application/octet-stream")
        fs = os.fstat(f.fileno())
        self.send_header("Content-Length", str(fs[6]))
        self.send_header("Last-Modified", self.date_time_string(fs.st_mtime))
        fname = os.path.basename(path)
        self.send_header("Last-Modified", self.date_time_string(fs.st_mtime))
        self.send_header("Content-Disposition", "filename=%s" % fname)
        self.end_headers()
        return f
