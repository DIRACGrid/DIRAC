#!/usr/bin/env python
import argparse
import os

import DIRAC
from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise


def parse_args():
    parser = argparse.ArgumentParser(description="Setup DIRAC CS for running integration tests with DiracX")
    parser.add_argument("--disable-vo", nargs="+", help="Disable a VO", default=[])
    parser.add_argument("--url", help="URL of the DiracX services")
    parser.add_argument("--credentials-dir", help="Directory where hostcert.pem/hostkey.pem can be found")
    args = parser.parse_args()

    DIRAC.initialize(
        host_credentials=(
            f"{args.credentials_dir}/hostcert.pem",
            f"{args.credentials_dir}/hostkey.pem",
        )
    )

    main(args.url, args.disable_vo)


def main(url: str, disabled_vos: list[str]):
    from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI

    csAPI = CSAPI()

    returnValueOrRaise(csAPI.createSection("DiracX"))

    if url:
        returnValueOrRaise(csAPI.setOption("DiracX/URL", url))

    if disabled_vos:
        returnValueOrRaise(csAPI.setOption("DiracX/DisabledVOs", ",".join(disabled_vos)))

    returnValueOrRaise(csAPI.commit())


if __name__ == "__main__":
    parse_args()
