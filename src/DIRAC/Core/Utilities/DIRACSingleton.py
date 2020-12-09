########################################################################
# File: DIRACSingleton.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/11/24 13:40:38
########################################################################

"""
:mod: DIRACSingleton

.. module: DIRACSingleton

:synopsis: metaclass singleton implementation

.. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

metaclass singleton implementation

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id $"

##
# @file DIRACSingleton.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/11/24 13:40:49
# @brief Definition of DIRACSingleton class.


class DIRACSingleton(type):
  """
  Simple singleton pattern using metaclass

  If you want make your class a singleton, just set its  __metaclass__ to
  DIRACSingleton, i.e.::

    import six
    from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton
    @six.add_metaclass(DIRACSingleton)
    class CheesShop(object):
      ...
  """
  def __init__(cls, name, bases, dic):
    """ c'tor

    :param cls: class def
    :param name: class name (becomes __name__ attr)
    :param bases: tuple of parent class defs (becomes __bases__ attr)
    :param dic: definition dict for class body (becomes __dict__ attr)
    """
    super(DIRACSingleton, cls).__init__(name, bases, dic)
    cls.instance = None

  def __call__(cls, *args, **kwargs):
    """
    Get the only one instance of cls

    :param cls: class def
    :param list args: anon args list
    :param dict kwargs: named args dict
    """
    if cls.instance is None:
      cls.instance = super(DIRACSingleton, cls).__call__(*args, **kwargs)
    return cls.instance
