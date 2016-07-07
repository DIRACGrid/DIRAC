"""
    Extremely simple utility class to send mails
"""

import os
import socket

from smtplib import SMTP
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from getpass import getuser

from DIRAC import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id$"

class Mail( object ):

  def __init__( self ):
    self._subject = ''
    self._message = ''
    self._mailAddress = ''
    self._html = False
    self._fromAddress = getuser() + '@' + socket.getfqdn()
    self._attachments = []
    self.esmtp_features = {}


  def _send( self ):

    if not self._mailAddress:
      gLogger.warn( "No mail address was provided. Mail not sent." )
      return S_ERROR( "No mail address was provided. Mail not sent." )

    if not self._message:
      gLogger.warn( "Message body is empty" )
      if not self._subject:
        gLogger.warn( "Subject and body empty. Mail not sent" )
        return S_ERROR ( "Subject and body empty. Mail not sent" )

    if self._html:
      mail = MIMEText( self._message , "html" )
    else:
      mail = MIMEText( self._message , "plain" )


    msg = MIMEMultipart()


    msg.attach( mail )


    addresses = self._mailAddress
    if isinstance( self._mailAddress, basestring ):
      addresses = self._mailAddress.split( ", " )

    msg[ "Subject" ] = self._subject
    msg[ "From" ] = self._fromAddress
    msg[ "To" ] = ', '.join( addresses )

    for attachment in self._attachments:
      try:
        with open( attachment, "rb" ) as fil:
          part = MIMEApplication( fil.read(),
                                  Name = os.path.basename( attachment )
                                )
          part['Content-Disposition'] = 'attachment; filename="%s"' % os.path.basename( attachment )
          msg.attach( part )
      except IOError as e:
        gLogger.exception( "Could not attach %s" % attachment, lException = e )

    smtp = SMTP()
    smtp.set_debuglevel( 0 )
    try:
      smtp.connect()
      smtp.sendmail( self._fromAddress, addresses, msg.as_string() )
    except Exception as x:
      return S_ERROR( "Sending mail failed %s" % str( x ) )

    smtp.quit()
    return S_OK( "The mail was successfully sent" )
