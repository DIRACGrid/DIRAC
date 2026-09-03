#!/usr/bin/env python
import argparse

from diraccfg import CFG

import DIRAC
from DIRAC.Core.Utilities.ReturnValues import returnValueOrRaise


def parse_args():
    parser = argparse.ArgumentParser(description="Setup DIRAC CS for running integration tests with DiracX")
    parser.add_argument("--disable-vo", nargs="+", help="Disable a VO", default=[])
    parser.add_argument("--url", help="URL of the DiracX services")
    parser.add_argument("--credentials-dir", help="Directory where hostcert.pem/hostkey.pem can be found")
    parser.add_argument("--legacy-adapted-services", nargs="+", help="Services that have legacy adaptors.", default=[])
    args = parser.parse_args()

    DIRAC.initialize(
        host_credentials=(
            f"{args.credentials_dir}/hostcert.pem",
            f"{args.credentials_dir}/hostkey.pem",
        )
    )

    main(args.url, args.disable_vo, args.legacy_adapted_services)


def main(url: str, disabled_vos: list[str], legacy_adapted_services: list[str]):
    from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI

    csAPI = CSAPI()

    returnValueOrRaise(csAPI.createSection("DiracX"))

    if url:
        returnValueOrRaise(csAPI.setOption("DiracX/URL", url))

    if disabled_vos:
        returnValueOrRaise(csAPI.setOption("DiracX/DisabledVOs", ",".join(disabled_vos)))

    csSync = {
        "CsSync": {
            "VOs": {
                "Jenkins": {
                    "DefaultGroup": "jenkins_user",
                    "IdP": {"ClientID": "995ed3b9-d5bd-49d3-a7f4-7fc7dbd5a0cd", "URL": "https://jenkins.invalid/"},
                    "UserSubjects": {
                        "adminusername": "e2cb28ec-1a1e-40ee-a56d-d899b79879ce",
                        "ciuser": "26dbe36e-cf5c-4c52-a834-29a1c904ef74",
                        "trialUser": "a95ab678-3fa4-41b9-b863-fe62ce8064ce",
                        "pilot": "879fd4c9-339b-4cbb-ae13-fc87053a5014",
                    },
                    "Support": {
                        "Message": "Contact the help desk",
                        "Email": "helpdesk@example.invalid",
                        "Webpage": "https://helpdesk.vo.invalid",
                    },
                },
                "vo": {
                    "DefaultGroup": "dirac_user",
                    "IdP": {"ClientID": "072afab5-ed92-46e0-a61d-4ecbc96e0770", "URL": "https://vo.invalid/"},
                    "UserSubjects": {
                        "adminusername": "26b14fc9-6d40-4ca5-b014-6234eaf0fb6e",
                        "ciuser": "d3adc733-6588-4d6f-8581-5986b02d0c87",
                        "trialUser": "ff2152ff-34f4-4739-b106-3def37e291e3",
                        "pilot": "e8be5ed6-2ba1-43cb-a7dd-b13944283467",
                    },
                },
            }
        }
    }

    csSyncCFG = CFG().loadFromDict(csSync)
    returnValueOrRaise(csAPI.mergeCFGUnderSection("/DiracX/", csSyncCFG))

    # Add service with legacy adaptors.
    legacy = {"FutureClientEnabled": {}}
    for service in legacy_adapted_services:
        print(f"Adding legacy adaptor for service {service}")
        # Service name such as:  system/name
        system, name = service.split("/")

        if not system in legacy["FutureClientEnabled"]:
            legacy["FutureClientEnabled"][system] = {}

        legacy["FutureClientEnabled"][system][name] = "yes"

    legacyCFG = CFG().loadFromDict(legacy)
    returnValueOrRaise(csAPI.mergeCFGUnderSection("/DiracX/", legacyCFG))

    returnValueOrRaise(csAPI.commit())


if __name__ == "__main__":
    parse_args()
