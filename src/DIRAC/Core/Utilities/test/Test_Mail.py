""".. module:: Test_Mail

Test cases for DIRAC.Core.Utilities.DAG module.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

#pylint: disable=protected-access,invalid-name,missing-docstring

import unittest

# sut
from  DIRAC.Core.Utilities.Mail import Mail

__RCSID__ = "$Id $"

########################################################################
class MailTestCase(unittest.TestCase):
  """ Test case for DIRAC.Core.Utilities.Mail module
  """
  pass

class MailEQ(MailTestCase):


  def test_createEmail(self):
    """ test _create
    """
    m = Mail()
    res = m._create('address@dirac.org')
    self.assertFalse(res['OK'])

    m._subject = 'subject'
    m._fromAddress = 'from@dirac.org'
    m._mailAddress = 'address@dirac.org'
    m._message = 'This is a message'
    res = m._create('address@dirac.org')
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'].__dict__['_headers'],
                     [('Content-Type', 'multipart/mixed'),
                      ('MIME-Version', '1.0'),
                      ('Subject', 'subject'),
                      ('From', 'from@dirac.org'),
                      ('To', 'address@dirac.org')])

  def test_compareEmails(self):
    """ test comparing of Email objects (also for insertion in sets)
    """
    m1 = Mail()
    m2 = Mail()
    self.assertEqual(m1, m2)

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
    self.assertEqual(m1, m2)
    m3 = Mail()
    m3._subject = 'subject'
    m3._fromAddress = 'from@dirac.org'
    m3._mailAddress = 'address@dirac.org'
    m3._message = 'This is a message a bit different'
    self.assertNotEqual(m1, m3)

    s = set()
    s.add(m1)
    s.add(m2)
    self.assertTrue(len(s) == 1)
    s.add(m2)
    self.assertTrue(len(s) == 1)
    s.add(m3)
    self.assertTrue(len(s) == 2)
    s.add(m3)
    self.assertTrue(len(s) == 2)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(MailTestCase)
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase(MailEQ))
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
