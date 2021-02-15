#!/usr/bin/env python
########################################################################
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

Examples::

  dirac-sys-sendmail "From: source@email.com\\nTo: destination@email.com\\nSubject: Test\\n\\nMessage body"
  echo "From: source@email.com\\nSubject: Test\\n\\nMessage body" | dirac-sys-sendmail destination@email.com
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"

import socket
import sys
import os

from DIRAC import gLogger, exit as DIRACexit
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient


@DIRACScript()
def main():
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  arg = "".join(args)

  if not len(arg) > 0:
    gLogger.error("Missing argument")
    DIRACexit(2)

  try:
    head, body = arg.split("\\n\\n")
  except Exception as x:
    head = "To: %s" % arg
    body = sys.stdin.read()

  try:
    tmp, body = body.split("\\n\\n")
    head = tmp + "\\n" + head
  except Exception as x:
    pass

  body = "".join(body.strip())

  try:
    headers = dict((i.strip(), j.strip()) for i, j in
                   (item.split(':') for item in head.split('\\n')))
  except BaseException:
    gLogger.error("Failed to convert string: %s to email headers" % head)
    DIRACexit(4)

  if "To" not in headers:
    gLogger.error("Failed to get 'To:' field from headers %s" % head)
    DIRACexit(5)
  to = headers["To"]

  origin = "%s@%s" % (os.getenv("LOGNAME", "dirac"), socket.getfqdn())
  if "From" in headers:
    origin = headers["From"]

  subject = "Sent from %s" % socket.getfqdn()
  if "Subject" in headers:
    subject = headers["Subject"]

  ntc = NotificationClient()
  print("sendMail(%s,%s,%s,%s,%s)" % (to, subject, body, origin, False))
  result = ntc.sendMail(to, subject, body, origin, localAttempt=False)
  if not result["OK"]:
    gLogger.error(result["Message"])
    DIRACexit(6)

  DIRACexit(0)


if __name__ == "__main__":
  main()
