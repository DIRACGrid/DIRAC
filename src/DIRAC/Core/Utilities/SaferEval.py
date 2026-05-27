#!/usr/bin/env python3

import ast


def saferEval(obj_str, max_len=2048):
    """This function adds an extra length check around literal_eval.
    On python3.11 and above (which has a recursion guard), this should
    be safe enough for use on general authenticated user input.

    Note: This doesn't handle all of the cases of eval, such as
          datetime as those are technically executing code to
          instantiate the non-base objects.
    """
    # Ensure input is a string
    obj_str = str(obj_str)
    if len(obj_str) > max_len:
        raise ValueError(f"Object string is too long (>{max_len} bytes)")
    try:
        return ast.literal_eval(obj_str)
    except (ValueError, TypeError, SyntaxError):
        # This covers all of the cases where the string is wrong (unclosed brackets...)
        # or contains disallowed items like function calls or non-expression.
        raise ValueError("Syntax error processing object expression")
    except (MemoryError, RecursionError):
        # This is encountered if the object is nested too deeply and other structures
        # that are probably malicious.
        raise ValueError("Object expression too large")
    except Exception:
        # There are no other possible exceptions at the time of writing,
        # this is to catch any added in future python versions.
        raise ValueError("Unknown error processing object expression")
