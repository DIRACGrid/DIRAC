"""
:mod: DIRACSingleton

.. module: DIRACSingleton
.. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

metaclass singleton implementation
"""


class DIRACSingleton(type):
    """
    Simple singleton pattern using metaclass

    If you want make your class a singleton, just set the keyword argument ``metaclass=DIRACSingleton`` in its definition i.e.::

      from DIRAC.Core.Utilities.DIRACSingleton import DIRACSingleton
      class CheeseShop(metaclass=DIRACSingleton):
          pass
    """

    def __init__(cls, name, bases, dic):
        """c'tor

        :param cls: class def
        :param name: class name (becomes __name__ attr)
        :param bases: tuple of parent class defs (becomes __bases__ attr)
        :param dic: definition dict for class body (becomes __dict__ attr)
        """
        super().__init__(name, bases, dic)
        cls.instance = None

    def __call__(cls, *args, **kwargs):
        """
        Get the only one instance of cls

        :param cls: class def
        :param list args: anon args list
        :param dict kwargs: named args dict
        """
        if cls.instance is None:
            cls.instance = super().__call__(*args, **kwargs)
        return cls.instance
