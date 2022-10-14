from DIRAC import S_ERROR, S_OK
from DIRAC.Core.Utilities import List
from diraccfg import CFG


def loadJDLAsCFG(jdl):
    """
    Load a JDL as CFG
    """

    def cleanValue(value):
        value = value.strip()
        if value[0] == '"':
            entries = []
            iPos = 1
            current = ""
            state = "in"
            while iPos < len(value):
                if value[iPos] == '"':
                    if state == "in":
                        entries.append(current)
                        current = ""
                        state = "out"
                    elif state == "out":
                        current = current.strip()
                        if current not in (",",):
                            return S_ERROR("value seems a list but is not separated in commas")
                        current = ""
                        state = "in"
                else:
                    current += value[iPos]
                iPos += 1
            if state == "in":
                return S_ERROR('value is opened with " but is not closed')
            return S_OK(", ".join(entries))
        else:
            return S_OK(value.replace('"', ""))

    def assignValue(key, value, cfg):
        key = key.strip()
        if not key:
            return S_ERROR("Invalid key name")
        value = value.strip()
        if not value:
            return S_ERROR("No value for key %s" % key)
        if value[0] == "{":
            if value[-1] != "}":
                return S_ERROR("Value '%s' seems a list but does not end in '}'" % (value))
            valList = List.fromChar(value[1:-1])
            for i, val in enumerate(valList):
                result = cleanValue(val)
                if not result["OK"]:
                    return S_ERROR(f"Var {key} : {result['Message']}")
                valList[i] = result["Value"]
                if valList[i] is None:
                    return S_ERROR(f"List value '{value}' seems invalid for item {i}")
            value = ", ".join(valList)
        else:
            result = cleanValue(value)
            if not result["OK"]:
                return S_ERROR(f"Var {key} : {result['Message']}")
            nV = result["Value"]
            if nV is None:
                return S_ERROR(f"Value '{value}' seems invalid")
            value = nV
        cfg.setOption(key, value)
        return S_OK()

    if jdl[0] == "[":
        iPos = 1
    else:
        iPos = 0
    key = ""
    value = ""
    action = "key"
    insideLiteral = False
    cfg = CFG()
    while iPos < len(jdl):
        char = jdl[iPos]
        if char == ";" and not insideLiteral:
            if key.strip():
                result = assignValue(key, value, cfg)
                if not result["OK"]:
                    return result
            key = ""
            value = ""
            action = "key"
        elif char == "[" and not insideLiteral:
            key = key.strip()
            if not key:
                return S_ERROR("Invalid key in JDL")
            if value.strip():
                return S_ERROR("Key %s seems to have a value and open a sub JDL at the same time" % key)
            result = loadJDLAsCFG(jdl[iPos:])
            if not result["OK"]:
                return result
            subCfg, subPos = result["Value"]
            cfg.createNewSection(key, contents=subCfg)
            key = ""
            value = ""
            action = "key"
            insideLiteral = False
            iPos += subPos
        elif char == "=" and not insideLiteral:
            if action == "key":
                action = "value"
                insideLiteral = False
            else:
                value += char
        elif char == "]" and not insideLiteral:
            key = key.strip()
            if len(key) > 0:
                result = assignValue(key, value, cfg)
                if not result["OK"]:
                    return result
            return S_OK((cfg, iPos))
        else:
            if action == "key":
                key += char
            else:
                value += char
                if char == '"':
                    insideLiteral = not insideLiteral
        iPos += 1

    return S_OK((cfg, iPos))
