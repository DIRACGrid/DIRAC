""" Test the ProxyPlugin class"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


import unittest
import mock
from DIRAC import S_OK, S_ERROR
from DIRAC.Resources.Catalog.ConditionPlugins.ProxyPlugin import ProxyPlugin

__RCSID__ = "$Id $"


def mock_getProxyInfo():
  return S_OK({'VOMS': ['/lhcb/Role=user'],
               'chain': '[/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=chaen/CN=123456/CN=Christophe Haen]',
               'group': 'lhcb_user',
               'groupProperties': ['NormalUser', 'SuperProperty'],
               'hasVOMS': True,
               'identity': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=chaen/CN=123456/CN=Christophe Haen',
               'isLimitedProxy': False,
               'isProxy': True,
               'issuer': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=chaen/CN=123456/CN=Christophe Haen/CN=proxy',
               'path': '/tmp/x509up_u12345',
               'secondsLeft': 86026,
               'subject': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=chaen/CN=123456/CN=Christophe Haen/CN=proxy/CN=proxy',  # noqa # pylint: disable=line-too-long
               'username': 'chaen',
               'validDN': True,
               'validGroup': True})


def mock_getNoProxyInfo():
  return S_ERROR("No proxy")


class TestProxyPlugin(unittest.TestCase):
  """ Test the FilenamePlugin class"""

  @mock.patch('DIRAC.Resources.Catalog.ConditionPlugins.ProxyPlugin.getProxyInfo', side_effect=mock_getProxyInfo)
  def test_01_username(self, _mockProxyInfo):
    """ Testing username attribute"""

    # the username is chaen
    self.assertTrue(not ProxyPlugin('username.in(toto)').eval())
    self.assertTrue(ProxyPlugin('username.not_in(toto)').eval())
    self.assertTrue(ProxyPlugin('username.in(toto, chaen)').eval())
    self.assertTrue(not ProxyPlugin('username.not_in(toto, chaen)').eval())

    # Testing some more formating with spaces and quotes

    self.assertTrue(not ProxyPlugin('username.in( toto )').eval())
    self.assertTrue(ProxyPlugin('username.not_in("toto")').eval())
    self.assertTrue(ProxyPlugin('username.in(toto,"chaen")').eval())
    self.assertTrue(not ProxyPlugin("username.not_in('toto' ,chaen)").eval())

  @mock.patch('DIRAC.Resources.Catalog.ConditionPlugins.ProxyPlugin.getProxyInfo', side_effect=mock_getProxyInfo)
  def test_02_group(self, _mockProxyInfo):
    """ Testing group attribute"""

    # the group is lhcb_user
    self.assertTrue(not ProxyPlugin('group.in(toto)').eval())
    self.assertTrue(ProxyPlugin('group.not_in(toto)').eval())
    self.assertTrue(ProxyPlugin('group.in(toto, lhcb_user)').eval())
    self.assertTrue(not ProxyPlugin('group.not_in(toto, lhcb_user)').eval())

    # Testing some more formating with spaces and quotes

    self.assertTrue(not ProxyPlugin('group.in( toto )').eval())
    self.assertTrue(ProxyPlugin('group.not_in("toto")').eval())
    self.assertTrue(ProxyPlugin('group.in(toto,"lhcb_user")').eval())
    self.assertTrue(not ProxyPlugin("group.not_in('toto' ,lhcb_user)").eval())

  @mock.patch('DIRAC.Resources.Catalog.ConditionPlugins.ProxyPlugin.getProxyInfo', side_effect=mock_getProxyInfo)
  def test_03_property(self, _mockProxyInfo):
    """ Testing property attribute"""

    # the properties are 'NormalUser', 'SuperProperty'
    self.assertTrue(not ProxyPlugin('property.has(toto)').eval())
    self.assertTrue(ProxyPlugin('property.has_not(toto)').eval())
    self.assertTrue(ProxyPlugin('property.has(SuperProperty)').eval())
    self.assertTrue(not ProxyPlugin('property.has_not(SuperProperty)').eval())

  @mock.patch('DIRAC.Resources.Catalog.ConditionPlugins.ProxyPlugin.getProxyInfo', side_effect=mock_getProxyInfo)
  def test_04_voms(self, _mockProxyInfo):
    """ Testing voms attribute"""

    # the voms role is /lhcb/Role=user
    self.assertTrue(not ProxyPlugin('voms.has(toto)').eval())
    self.assertTrue(ProxyPlugin('voms.has_not(toto)').eval())
    self.assertTrue(ProxyPlugin('voms.has(/lhcb/Role->user)').eval())
    self.assertTrue(not ProxyPlugin('voms.has_not(/lhcb/Role->user)').eval())

  @mock.patch('DIRAC.Resources.Catalog.ConditionPlugins.ProxyPlugin.getProxyInfo', side_effect=mock_getProxyInfo)
  def test_05_errors(self, _mockProxyInfo):
    """ Testing errors handling"""

    # Non existing attribute
    with self.assertRaises(RuntimeError):
      ProxyPlugin('boom.boom(something)').eval()

    # Non matching predicate with the attribute
    with self.assertRaises(RuntimeError):
      ProxyPlugin('username.boom(something)').eval()

    with self.assertRaises(RuntimeError):
      ProxyPlugin('group.boom(something)').eval()

    with self.assertRaises(RuntimeError):
      ProxyPlugin('property.boom(something)').eval()

    with self.assertRaises(RuntimeError):
      ProxyPlugin('voms.boom(something)').eval()

    # impossible parsing
    with self.assertRaises(AttributeError):
      ProxyPlugin('cannotbeparsed').eval()

  @mock.patch('DIRAC.Resources.Catalog.ConditionPlugins.ProxyPlugin.getProxyInfo', side_effect=mock_getNoProxyInfo)
  def test_05_withoutProxy(self, _mockProxyInfo):
    """ Testing without proxy, everything should be False"""

    # A priori, not both of them can give the same result
    # but since there is no proxy, it should !
    self.assertFalse(ProxyPlugin('username.in(toto)').eval())
    self.assertFalse(ProxyPlugin('username.not_in(toto)').eval())


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestProxyPlugin)

  unittest.TextTestRunner(verbosity=2).run(suite)
