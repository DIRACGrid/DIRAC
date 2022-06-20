"""This class is wrapper around ClassAd to see if an element has been modified"""

from typing import Iterable

from DIRAC.Core.Utilities.ClassAd import ClassAd


class JobManifest(ClassAd):
    def __init__(self, jdl: str = ""):
        super().__init__(jdl)
        self.__dirty = False

    # New methods

    def isDirty(self):
        """Get in which state the jdl is"""
        return self.__dirty

    def setDirty(self):
        """Set the JDL to the dirty state"""
        self.__dirty = True

    def clearDirty(self):
        """Clean the JDL from the dirty state"""
        self.__dirty = False

    # Overriden methods

    def insertAttributeInt(self, name: str, attribute: int) -> None:
        """Insert a named integer attribute"""
        self.setDirty()
        super().insertAttributeInt(name, attribute)

    def insertAttributeBool(self, name: str, attribute: bool) -> None:
        """Insert a named boolean attribute"""
        self.setDirty()
        super().insertAttributeBool(name, attribute)

    def insertAttributeString(self, name: str, attribute: str) -> None:
        """Insert a named string attribute"""
        self.setDirty()
        super().insertAttributeString(name, attribute)

    def insertAttributeSubsection(self, name: str, attribute) -> None:
        """Insert a ClassAd attribute in the jdl"""
        self.setDirty()
        super().insertAttributeSubsection(name, attribute)

    def insertAttributeVectorString(self, name, attributelist: Iterable[str]) -> None:
        """Insert a named string list attribute"""
        self.setDirty()
        super().insertAttributeVectorString(name, attributelist)

    def insertAttributeVectorInt(self, name, attributelist: Iterable[int]) -> None:
        """Insert a named string list attribute"""
        self.setDirty()
        super().insertAttributeVectorInt(name, attributelist)

    def insertAttributeVectorStringList(self, name, attributelist: Iterable[str]) -> None:
        """Insert a named list of string lists"""
        self.setDirty()
        super().insertAttributeVectorStringList(name, attributelist)

    def set_expression(self, name: str, attribute: str) -> None:
        """Insert a named expression attribute"""
        self.setDirty()
        super().set_expression(name, attribute)

    def deleteAttribute(self, name: str) -> bool:
        """Delete a named attribute"""
        self.setDirty()
        return super().deleteAttribute(name)
