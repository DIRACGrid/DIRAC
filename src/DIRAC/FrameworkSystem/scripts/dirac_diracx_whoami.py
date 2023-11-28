"""Query DiracX for information about the current user

This is a stripped down version of the "dirac whoami" script from DiracX.
It primarily exists as a method of validating the current user's credentials are functional.
"""
import json

from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Security.DiracX import DiracXClient


@Script()
def main():
    Script.parseCommandLine()

    try:
        with DiracXClient() as api:
            user_info = api.auth.userinfo()
            print(json.dumps(user_info.as_dict(), indent=2))
    except Exception as e:
        print(f"Failed to access DiracX: {e}")


if __name__ == "__main__":
    main()
