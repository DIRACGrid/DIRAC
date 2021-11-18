"""
    Workflow Utility module contains a number of functions useful for various
    workflow operations
"""
import re


def getSubstitute(param, skip_list=[]):
    """Get the variable name to which the given parameter is referring"""
    result = []
    resList = re.findall(r"@{([][\w,.:$()]+)}", str(param))
    if resList:
        for match in resList:
            if match not in skip_list:
                result.append(match)

    return result


def substitute(param, variable, value):
    """Substitute the variable reference with the value"""

    tmp_string = str(param).replace("@{" + variable + "}", value)
    if isinstance(param, str):
        return tmp_string
    return eval(tmp_string)


def resolveVariables(varDict):
    """Resolve variables defined in terms of others within the same dictionary"""
    max_tries = 10
    variables = list(varDict)
    ntry = 0
    while ntry < max_tries:
        substFlag = False
        for var, value in list(varDict.items()):
            if isinstance(value, str):
                substitute_vars = getSubstitute(value)
                for substitute_var in substitute_vars:
                    if substitute_var in variables:
                        varDict[var] = substitute(varDict[var], substitute_var, varDict[substitute_var])
                        substFlag = True
        if not substFlag:
            break
        ntry += 1
    else:
        print("Failed to resolve referencies in %d attempts" % max_tries)


def dataFromOption(parameter):

    result = []

    if parameter.type.lower() == "option":

        fields = parameter.value.split(",")

        for f in fields:
            if re.search(r"FILE\s*=", f):
                fname = re.search(r"FILE\s*=\s*'([][;\/\w.:\s@{}-]+)'", f).group(1)
                res = re.search(r"TYP\w*\s*=\s*'(\w+)'", f)
                if res:
                    ftype = res.group(1)
                else:
                    ftype = "Unknown"

                result.append((fname, ftype))

    return result


def expandDatafileOption(option):

    result = ""

    if not re.search(";;", option.value):
        return result

    files = dataFromOption(option)
    if len(files) == 1:
        fname, ftype = files[0]
        fnames = fname.split(";;")
        if len(fnames) > 1:

            template = option.value.strip().replace("=", "", 1)
            template = template.replace("{", "")
            template = template.replace("}", "")
            opt = []
            for f in fnames:
                opt.append(template.replace(fname, f))

            result = "={" + ",".join(opt) + "}"

    return result
