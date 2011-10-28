# $HeadURL$
"""
    Extremely simple utility class to send mails
"""
__RCSID__ = "$Id$"

import socket
from smtplib import SMTP
from getpass import getuser
from DIRAC import gLogger, S_OK, S_ERROR

class Mail:

  def __init__( self ):
    self._subject = ''
    self._message = ''
    self._mailAddress = ''
    self._fromAddress = getuser() + '@' + socket.getfqdn()
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

    mailString = "From: %s\nTo: %s\nSubject: %s\n%s\n"
    addresses = self._mailAddress
    if not type( self._mailAddress ) == type( [] ):
      addresses = [self._mailAddress]

    text = mailString % ( self._fromAddress, ', '.join( addresses ),
                          self._subject, self._message )

    smtp = SMTP()
    smtp.set_debuglevel( 0 )
    try:
      #smtp.connect( self._hostname )
      smtp.connect()
      smtp.sendmail( self._fromAddress, self._mailAddress, text )
    except Exception, x:
      return S_ERROR( "Sending mail failed %s" % str( x ) )

    smtp.quit()
    return S_OK( "The mail was succesfully sent" )
