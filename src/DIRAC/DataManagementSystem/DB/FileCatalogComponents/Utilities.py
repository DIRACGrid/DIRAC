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


def executeInTransaction(db, func):
    """Execute ``func(cursor)`` within a single transaction.

    The connections of the pool are in autocommit mode, so the transaction has to be
    explicitly started, and all the statements have to be executed on the same cursor.
    If ``func`` raises, the transaction is rolled back.

    :param db: MySQL database object
    :param func: callable taking a cursor as argument, executing the statements on it

    :returns: S_OK(return value of func) or S_ERROR
    """
    res = db._getConnection()
    if not res["OK"]:
        return res
    conn = res["Value"]

    try:
        with conn.cursor() as cursor:
            cursor.execute("START TRANSACTION")
            value = func(cursor)
        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass  # nosec B110
        return db._except("executeInTransaction", e, "Transaction failed.")

    return S_OK(value)
