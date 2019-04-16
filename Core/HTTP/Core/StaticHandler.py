
import os
from tornado.web import StaticFileHandler, HTTPError

from DIRAC import rootPath

class StaticHandler( StaticFileHandler ):

  def initialize( self, pathList, default_filename = None ):
    self.pathList = [ os.path.abspath( path ) + os.path.sep for path in pathList ]
    self.default_filename = default_filename
    self.root = rootPath

  def parse_url_path( self, url_path ):
    if os.path.sep != "/":
      url_path = url_path.replace( "/", os.path.sep )
    for possiblePath in self.pathList:
      possiblePath = os.path.join( possiblePath, url_path )
      if self.default_filename and os.path.isdir( possiblePath ):
        possiblePath = os.path.join( possiblePath, self.default_filename )
      if os.path.isfile( possiblePath ):
        return possiblePath
    raise HTTPError( 404 )





