#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-sys-sendmail
# Author :  Matvey Sapunov
########################################################################

"""
  Utility to send an e-mail using DIRAC notification service.

  Arguments:
    Formated text message. The message consists of e-mail headers and e-mail body
    separated by two newline characters. Headers are key : value pairs separated
    by newline character. Meaningful headers are "To:", "From:", "Subject:".
    Other keys will be ommited.
    Message body is an arbitrary string.
    

  Options:
    There are no options.

  Example:
    dirac-sys-sendmail "From: source@email.com\\nTo: destination@email.com\\nSubject: Test\\n\\nMessage body"
"""

__RCSID__ = "$Id$"

import socket
import DIRAC

from DIRAC                                              import gLogger
from DIRAC.Core.Base                                    import Script
from DIRAC.FrameworkSystem.Client.NotificationClient    import NotificationClient

Script.setUsageMessage( ''.join( __doc__ ) )

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

arg = "".join( args )

try:
  head , body = arg.split( "\\n\\n" )
except Exception , x:
  gLogger.error( "Failed to get e-mail header and body from: %s" % arg )
  DIRAC.exit( 2 )

body = "".join( body )

try:
  headers = dict( ( i.strip() , j.strip()) for i , j in 
              ( item.split( ':' ) for item in head.split( '\\n' ) ) )
except:
  gLogger.error( "Failed to convert string: %s to email headers" % head )
  DIRAC.exit( 3 )

if not "To" in headers:
  gLogger.error( "Failed to get 'To:' field from headers %s" % head )
  DIRAC.exit( 4 )
to = headers[ "To" ]

origin = socket.gethostname()
if "From" in headers:
  origin = headers[ "From" ]

subject = ""
if "Subject" in headers:
  subject = headers[ "Subject" ]

ntc = NotificationClient()
result = ntc.sendMail( to , subject , body , origin , localAttempt = True )
if not result[ "OK" ]:
  gLogger.error( result[ "Message" ] )
  DIRAC.exit( 5 )

DIRAC.exit( 0 )
