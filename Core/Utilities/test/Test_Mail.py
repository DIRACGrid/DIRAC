""".. module:: Test_Mail

Test cases for DIRAC.Core.Utilities.DAG module.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from DIRAC.Core.Utilities.Mail import Mail

__RCSID__ = "$Id $"


def test_createEmail():
  m = Mail()
  res = m._create('address@dirac.org')
  assert not res['OK']

  m._subject = 'subject'
  m._fromAddress = 'from@dirac.org'
  m._mailAddress = 'address@dirac.org'
  m._message = 'This is a message'
  res = m._create('address@dirac.org')
  assert res['OK']
  assert res['Value'].__dict__['_headers'] == [
      ('Content-Type', 'multipart/mixed'),
      ('MIME-Version', '1.0'),
      ('Subject', 'subject'),
      ('From', 'from@dirac.org'),
      ('To', 'address@dirac.org')
  ]


def test_compareEmails(monkeypatch):
  # The hostname on GitHub actions can change randomly so mock it
  monkeypatch.setattr("socket.getfqdn", lambda: "localhost.example")

  m1 = Mail()
  m2 = Mail()
  assert m1 == m2, (m1.__dict__, m2.__dict__)

  m1 = Mail()
  m1._subject = 'subject'
  m1._fromAddress = 'from@dirac.org'
  m1._mailAddress = 'address@dirac.org'
  m1._message = 'This is a message'
  m2 = Mail()
  m2._subject = 'subject'
  m2._fromAddress = 'from@dirac.org'
  m2._mailAddress = 'address@dirac.org'
  m2._message = 'This is a message'
  assert m1 == m2
  m3 = Mail()
  m3._subject = 'subject'
  m3._fromAddress = 'from@dirac.org'
  m3._mailAddress = 'address@dirac.org'
  m3._message = 'This is a message a bit different'
  assert m1 != m3

  s = {m1, m2}
  assert len(s) == 1
  s.add(m2)
  assert len(s) == 1
  s.add(m3)
  assert len(s) == 2
  s.add(m3)
  assert len(s) == 2
