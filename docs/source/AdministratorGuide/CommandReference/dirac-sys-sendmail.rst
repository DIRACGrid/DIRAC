.. _admin_dirac-sys-sendmail:

==================
dirac-sys-sendmail
==================

Utility to send an e-mail using DIRAC notification service.

  Arguments::

    Formated text message. The message consists of e-mail headers and e-mail body
    separated by two newline characters. Headers are key : value pairs separated
    by newline character. Meaningful headers are "To:", "From:", "Subject:".
    Other keys will be ommited.
    Message body is an arbitrary string.

  Options::

    There are no options.

  Examples::

    dirac-sys-sendmail "From: source@email.com\nTo: destination@email.com\nSubject: Test\n\nMessage body"
    echo "From: source@email.com\nSubject: Test\n\nMessage body" | dirac-sys-sendmail destination@email.com
