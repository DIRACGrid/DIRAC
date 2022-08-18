""" ClassAd Class - a light purely Python representation of the
    Condor ClassAd library.
"""


class ClassAd:
    def __init__(self, jdl):
        """ClassAd constructor from a JDL string"""
        self.contents = {}
        result = self.__analyse_jdl(jdl)
        if result:
            self.contents = result

    def __analyse_jdl(self, jdl, index=0):
        """Analyse one [] jdl enclosure"""

        jdl = jdl.strip()

        # Strip all the blanks first
        # temp = jdl.replace(' ','').replace('\n','')
        temp = jdl

        result = {}

        if temp[0] != "[" or temp[-1] != "]":
            print("Invalid JDL: it should start with [ and end with ]")
            return result

        # Parse the jdl string now
        body = temp[1:-1]
        index = 0
        namemode = 1
        valuemode = 0
        while index < len(body):
            if namemode:
                ind = body.find("=", index)
                if ind != -1:
                    name = body[index:ind]
                    index = ind + 1
                    valuemode = 1
                    namemode = 0
                else:
                    break
            elif valuemode:
                ind1 = body.find("[", index)
                ind2 = body.find(";", index)
                if ind1 != -1 and ind1 < ind2:
                    value, newind = self.__find_subjdl(body, ind1)
                elif ind1 == -1 and ind2 == -1:
                    value = body[index:]
                    newind = len(body)
                else:
                    if index == ind2:
                        return {}
                    else:
                        value = body[index:ind2]
                        newind = ind2 + 1

                result[name.strip()] = value.strip().replace("\n", "")
                index = newind
                valuemode = 0
                namemode = 1

        return result

    def __find_subjdl(self, body, index):
        """Find a full [] enclosure starting from index"""
        result = ""
        if body[index] != "[":
            return (result, 0)

        depth = 0
        ind = index
        while depth < 10:
            ind1 = body.find("]", ind + 1)
            ind2 = body.find("[", ind + 1)
            if ind2 != -1 and ind2 < ind1:
                depth += 1
                ind = ind2
            else:
                if depth > 0:
                    depth -= 1
                    ind = ind1
                else:
                    result = body[index : ind1 + 1]
                    if body[ind1 + 1] == ";":
                        return (result, ind1 + 2)
                    return result, 0

        return result, 0

    def insertAttributeInt(self, name, attribute):
        """Insert a named integer attribute"""

        self.contents[name] = str(attribute)

    def insertAttributeBool(self, name, attribute):
        """Insert a named boolean attribute"""

        if attribute:
            self.contents[name] = "true"
        else:
            self.contents[name] = "false"

    def insertAttributeString(self, name, attribute):
        """Insert a named string attribute"""

        self.contents[name] = '"' + str(attribute) + '"'

    def insertAttributeVectorString(self, name, attributelist):
        """Insert a named string list attribute"""

        tmp = ['"' + x + '"' for x in attributelist]
        tmpstr = ",".join(tmp)
        self.contents[name] = "{" + tmpstr + "}"

    def insertAttributeVectorInt(self, name, attributelist):
        """Insert a named string list attribute"""

        tmp = [str(x) for x in attributelist]
        tmpstr = ",".join(tmp)
        self.contents[name] = "{" + tmpstr + "}"

    def insertAttributeVectorStringList(self, name, attributelist):
        """Insert a named list of string lists"""

        listOfLists = []
        for stringList in attributelist:
            # tmp = map ( lambda x : '"' + x + '"', stringList )
            tmpstr = ",".join(stringList)
            listOfLists.append("{" + tmpstr + "}")
        self.contents[name] = "{" + ",".join(listOfLists) + "}"

    def lookupAttribute(self, name):
        """Check the presence of the given attribute"""

        return name in self.contents

    def set_expression(self, name, attribute):
        """Insert a named expression attribute"""

        self.contents[name] = str(attribute)

    def get_expression(self, name):
        """Get expression corresponding to a named attribute"""

        if name in self.contents:
            if isinstance(self.contents[name], int):
                return str(self.contents[name])
            return self.contents[name]
        return ""

    def isAttributeList(self, name):
        """Check if the given attribute is of the List type"""
        attribute = self.get_expression(name).strip()
        return attribute.startswith("{")

    def getListFromExpression(self, name):
        """Get a list of strings from a given expression"""

        tempString = self.get_expression(name).strip()
        listMode = False
        if tempString.startswith("{"):
            tempString = tempString[1:-1]
            listMode = True

        tempString = tempString.replace(" ", "").replace("\n", "")
        if tempString.find("{") < 0:
            if not listMode:
                tempString = tempString.replace('"', "")
                if not tempString:
                    return []
                return tempString.split(",")

        resultList = []
        while tempString:
            if tempString.find("{") == 0:
                end = tempString.find("}")
                resultList.append(tempString[: end + 1])
                tempString = tempString[end + 1 :]
                if tempString.startswith(","):
                    tempString = tempString[1:]
            elif tempString.find('"') == 0:
                end = tempString[1:].find('"')
                resultList.append(tempString[1 : end + 1])
                tempString = tempString[end + 2 :]
                if tempString.startswith(","):
                    tempString = tempString[1:]
            else:
                end = tempString.find(",")
                if end < 0:
                    resultList.append(tempString.replace('"', "").replace(" ", ""))
                    break
                else:
                    resultList.append(tempString[:end].replace('"', "").replace(" ", ""))
                    tempString = tempString[end + 1 :]

        return resultList

    def getDictionaryFromSubJDL(self, name):
        """Get a dictionary of the JDL attributes from a subsection"""

        tempList = self.get_expression(name)[1:-1]
        resDict = {}
        for item in tempList.split(";"):
            if len(item.split("=")) == 2:
                resDict[item.split("=")[0].strip()] = item.split("=")[1].strip().replace('"', "")
            else:
                return {}

        return resDict

    def deleteAttribute(self, name):
        """Delete a named attribute"""

        if name in self.contents:
            del self.contents[name]
            return 1
        return 0

    def isOK(self):
        """Check the JDL validity - to be defined"""

        if self.contents:
            return 1
        return 0

    def asJDL(self):
        """Convert the JDL description into a string"""

        result = []
        for name, value in sorted(self.contents.items()):
            if value[0:1] == "{":
                result += [4 * " " + name + " = \n"]
                result += [8 * " " + "{\n"]
                strings = value[1:-1].split(",")
                for st in strings:
                    result += [12 * " " + st.strip() + ",\n"]
                result[-1] = result[-1][:-2]
                result += ["\n" + 8 * " " + "};\n"]
            elif value[0:1] == "[":
                tempad = ClassAd(value)
                tempjdl = tempad.asJDL() + ";"
                lines = tempjdl.split("\n")
                result += [4 * " " + name + " = \n"]
                for line in lines:
                    result += [8 * " " + line + "\n"]

            else:
                result += [4 * " " + name + " = " + str(value) + ";\n"]
        if result:
            result[-1] = result[-1][:-1]
        return "[ \n" + "".join(result) + "\n]"

    def getAttributeString(self, name):
        """Get String type attribute value"""
        value = ""
        if self.lookupAttribute(name):
            value = self.get_expression(name).replace('"', "")
        return value

    def getAttributeInt(self, name):
        """Get Integer type attribute value"""
        value = None
        if self.lookupAttribute(name):
            try:
                value = int(self.get_expression(name).replace('"', ""))
            except Exception:
                value = None
        return value

    def getAttributeBool(self, name):
        """Get Boolean type attribute value"""
        if not self.lookupAttribute(name):
            return False

        value = self.get_expression(name).replace('"', "")
        return value.lower() == "true"

    def getAttributeFloat(self, name):
        """Get Float type attribute value"""
        value = None
        if self.lookupAttribute(name):
            try:
                value = float(self.get_expression(name).replace('"', ""))
            except Exception:
                value = None
        return value

    def getAttributes(self):
        """Get the list of all the attribute names

        :return: list of names as strings
        """
        return list(self.contents)
