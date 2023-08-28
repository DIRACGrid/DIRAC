""" Utility for prompting users
"""
from DIRAC import S_OK, S_ERROR


def promptUser(message, choices=[], default="n", logger=None):
    """Prompting users with message, choices by default are 'y', 'n'"""
    if logger is None:
        from DIRAC import gLogger

        logger = gLogger

    if not choices:
        choices = ["y", "n"]
    if (choices) and (default) and (default not in choices):
        return S_ERROR("The default value is not a valid choice")
    choiceString = ""
    if choices and default:
        choiceString = "/".join(choices).replace(default, f"[{default}]")
    elif choices and (not default):
        choiceString = "/".join(choices)
    elif (not choices) and (default):
        choiceString = f"[{default}]"

    while True:
        if choiceString:
            logger.notice(f"{message} {choiceString} :")
        elif default:
            logger.notice(f"{message} {default} :")
        else:
            logger.notice(f"{message} :")
        response = input("")
        if (not response) and (default):
            return S_OK(default)
        elif (not response) and (not default):
            logger.error("Failed to determine user selection")
            return S_ERROR("Failed to determine user selection")
        elif (response) and (choices) and (response not in choices):
            logger.notice("your answer is not valid")
            continue
        else:
            return S_OK(response)
