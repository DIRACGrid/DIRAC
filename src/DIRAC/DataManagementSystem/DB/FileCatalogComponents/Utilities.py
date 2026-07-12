""" DIRAC FileCatalog utilities
"""
from DIRAC import S_OK, S_ERROR


def getIDSelectString(ids):
    """
    :param ids: input IDs - can be single int, list or tuple or a SELECT string
    :return: Select string
    """
    if isinstance(ids, str) and ids.lower().startswith("select"):
        idString = ids
    elif isinstance(ids, int):
        idString = "%d" % ids
    elif isinstance(ids, (tuple, list)):
        # cast to int to minimise SQL injection risk
        idString = ",".join([f"{int(x)}" for x in ids])
    else:
        return S_ERROR("Illegal fileID")

    return S_OK(idString)
