########################################################################
# File: Graph.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/09/27 07:22:15
########################################################################
"""
:mod: Graph

.. module: Graph

:synopsis: graph

.. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

graph
"""

# #
# @file Graph.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/09/27 07:22:23
# @brief Definition of Graph class.
# pylint: disable=no-member


class DynamicProps(type):
    """

    metaclass allowing to create properties on the fly
    """

    def __new__(cls, name, bases, classdict):
        """
        new operator
        """

        def makeProperty(self, name, value, readOnly=False):
            """
            Add property :name: to class

            This also creates a private :_name: attribute
            If you want to make read only property, set :readOnly: flag to True
            :warn: could raise AttributeError if :name: of :_name: is already
            defined as an attribute
            """
            if hasattr(self, "_" + name) or hasattr(self, name):
                raise AttributeError("_%s or %s is already defined as a member" % (name, name))

            def fget(self):
                return self._getProperty(name)

            fset = None if readOnly else lambda self, value: self._setProperty(name, value)
            setattr(self, "_" + name, value)
            setattr(self.__class__, name, property(fget=fget, fset=fset))

        def _setProperty(self, name, value):
            """
            property setter
            """
            setattr(self, "_" + name, value)

        def _getProperty(self, name):
            """
            property getter
            """
            return getattr(self, "_" + name)

        classdict["makeProperty"] = makeProperty
        classdict["_setProperty"] = _setProperty
        classdict["_getProperty"] = _getProperty
        return super().__new__(cls, name, bases, classdict)
