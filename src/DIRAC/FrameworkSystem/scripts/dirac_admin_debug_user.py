#!/usr/bin/env python
"""Help to understand why a user is unable to get a proxy.

The user can be given as a certificate DN (as printed by dirac-proxy-init or
"openssl x509 -subject"), a DIRAC username, a CERN account, an email address or
a CERN person ID. If there is no exact match, similar users from the registry
are shown instead.

As an unregistered user cannot query the configuration, this command is
intended to be run by a colleague on behalf of the user having problems.

Example:
  $ dirac-admin-debug-user '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=jdoe/CN=123456/CN=Jane Doe'
  $ dirac-admin-debug-user --ca '/DC=ch/DC=cern/CN=CERN Grid Certification Authority' jdoe
"""

import re
from datetime import date, datetime, timedelta, timezone
from difflib import SequenceMatcher

from DIRAC import gConfig, exit as dexit
from DIRAC.Core.Base.Script import Script

MIN_SCORE = 0.75
# Scores at or above this mean the user is very likely the same person
STRONG_SCORE = 0.95
# Proxy CNs are appended as large random integers, much longer than CERN person IDs
PROXY_CN_RE = re.compile(r"/CN=(\d{8,}|proxy|limited proxy)$")


def normaliseDN(dn):
    """Convert the various ways a DN can be written to the slash separated form used by DIRAC"""
    dn = dn.strip()
    if dn.startswith(("subject=", "issuer=")):
        dn = dn.split("=", 1)[1].strip()
    if not dn.startswith("/"):
        # OpenSSL 3 style: "DC = ch, DC = cern, OU = Organic Units, CN = Jane Doe"
        dn = "/" + "/".join(re.sub(r"\s*=\s*", "=", part.strip()) for part in dn.split(","))
    stripped = False
    while match := PROXY_CN_RE.search(dn):
        dn = dn[: match.start()]
        stripped = True
    return dn, stripped


def splitDN(dn):
    """Split a slash separated DN into a list of (key, value) tuples"""
    return [tuple(part.split("=", 1)) for part in dn.strip("/").split("/") if "=" in part]


def nameFromDN(dn):
    """Get the last non numeric CN which, for most CAs, is the person's name"""
    names = [value for key, value in splitDN(dn) if key == "CN" and not value.isdigit()]
    return names[-1].lower() if names else ""


def loadUsers():
    users = {}
    result = gConfig.getSections("/Registry/Users")
    if not result["OK"]:
        raise RuntimeError(result["Message"])
    for username in result["Value"]:
        path = f"/Registry/Users/{username}"
        users[username] = {
            "DN": gConfig.getValue(f"{path}/DN", []),
            "CA": gConfig.getValue(f"{path}/CA", []),
            "Email": gConfig.getValue(f"{path}/Email", ""),
            # "None" is used as a placeholder by the synchronisation
            "Suspended": [vo for vo in gConfig.getValue(f"{path}/Suspended", []) if vo != "None"],
            "PrimaryCERNAccount": gConfig.getValue(f"{path}/PrimaryCERNAccount", ""),
            "CERNAccountType": gConfig.getValue(f"{path}/CERNAccountType", ""),
            "CERNPersonId": gConfig.getValue(f"{path}/CERNPersonId", ""),
            "AffiliationEnds": gConfig.getOptionsDict(f"{path}/AffiliationEnds").get("Value", {}),
        }
    return users


def scoreDN(queryDN, info):
    """Return (score, reason) for how likely it is that a user corresponds to the given DN"""
    queryCNs = {value.lower() for key, value in splitDN(queryDN) if key == "CN"}
    queryName = nameFromDN(queryDN)
    best = (0.0, "")
    for dn in info["DN"]:
        if dn == queryDN:
            return 1.0, "identical DN"
        # Only compare the names as the rest of the DN is often identical for everyone with the same CA
        best = max(best, (SequenceMatcher(None, queryName, nameFromDN(dn)).ratio(), "similar name"))
    # A matching CERN person ID or account name is a much stronger signal than the name
    if info["CERNPersonId"] and info["CERNPersonId"] in queryCNs:
        best = max(best, (0.98, f"same CERN person ID ({info['CERNPersonId']})"))
    if info["PrimaryCERNAccount"] and info["PrimaryCERNAccount"].lower() in queryCNs:
        best = max(best, (0.95, f"same CERN account ({info['PrimaryCERNAccount']})"))
    return best


def findUsers(query, users, limit):
    """Find the users matching a query, returns (exact, similar, normalisedDN)"""
    if query.startswith("/") or "=" in query:
        dn, stripped = normaliseDN(query)
        if stripped:
            print(f"NOTE: Removed proxy CN(s) from the DN, using {dn}\n")
        scored = sorted(
            ((*scoreDN(dn, info), username) for username, info in users.items()),
            reverse=True,
        )
        exact = [username for score, _, username in scored if score == 1.0]
        similar = [(username, score, reason) for score, reason, username in scored if MIN_SCORE <= score < 1.0]
        return exact, similar[:limit], dn

    q = query.strip().lower()
    exact = [
        username
        for username, info in users.items()
        if q in (username.lower(), info["Email"].lower(), info["PrimaryCERNAccount"].lower(), info["CERNPersonId"])
    ]
    similar = []
    if not exact:
        scored = sorted(
            (
                (
                    max(SequenceMatcher(None, q, s.lower()).ratio() for s in (username, info["Email"].split("@")[0])),
                    username,
                )
                for username, info in users.items()
            ),
            reverse=True,
        )
        similar = [(username, score, "similar username/email") for score, username in scored if score >= MIN_SCORE]
    return exact, similar[:limit], None


def canReadProxyDB():
    from DIRAC.Core.Security import Properties
    from DIRAC.Core.Security.ProxyInfo import getProxyInfo

    result = getProxyInfo()
    if not result["OK"]:
        return False
    return Properties.PROXY_MANAGEMENT in result["Value"].get("groupProperties", [])


def getUploadedProxies(username):
    from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager

    result = gProxyManager.getDBContents({"UserName": [username]})
    if not result["OK"]:
        return result
    names = result["Value"]["ParameterNames"]
    return {"OK": True, "Value": [dict(zip(names, record)) for record in result["Value"]["Records"]]}


def checkUser(username, info, queryDN, queryCA, checkProxies):
    """Print the details of a user and return a list of problems found"""
    from DIRAC.ConfigurationSystem.Client.Helpers import Registry

    problems = []
    groups = Registry.getGroupsForUser(username).get("Value", [])

    print(f"* {username}")
    for dn in info["DN"]:
        print(f"    DN            : {dn}{'  <-- matches' if dn == queryDN else ''}")
    for ca in info["CA"]:
        print(f"    CA            : {ca}")
    print(f"    Email         : {info['Email'] or '-'}")
    if info["PrimaryCERNAccount"]:
        print(f"    CERN account  : {info['PrimaryCERNAccount']} ({info['CERNAccountType'] or 'unknown type'})")
    if info["CERNPersonId"]:
        print(f"    CERN person ID: {info['CERNPersonId']}")
    print(f"    Groups        : {', '.join(groups) or '-'}")

    if queryCA and queryCA not in info["CA"]:
        problems.append(f"the given CA is not registered for this user (registered: {', '.join(info['CA']) or '-'})")
    if not groups:
        problems.append("the user is not a member of any group")
    if info["Suspended"]:
        problems.append(f"the user is suspended in: {', '.join(info['Suspended'])}")

    today = date.today()
    for vo, endDate in info["AffiliationEnds"].items():
        print(f"    Affiliation   : {vo} until {endDate}")
        try:
            endDate = date.fromisoformat(endDate)
        except ValueError:
            continue
        if endDate < today:
            problems.append(f"the {vo} affiliation ended on {endDate}")
        elif endDate < today + timedelta(days=30):
            problems.append(f"the {vo} affiliation ends soon ({endDate})")

    if checkProxies:
        result = getUploadedProxies(username)
        if not result["OK"]:
            print(f"    Proxies       : failed to query ProxyManager: {result['Message']}")
        elif not result["Value"]:
            print("    Proxies       : none uploaded")
            problems.append("no proxy has been uploaded to the ProxyManager")
        else:
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            validProxies = []
            for proxy in result["Value"]:
                expiry = proxy["ExpirationTime"]
                state = "valid" if expiry > now else "EXPIRED"
                print(f"    Proxy         : {proxy['UserDN']} until {expiry:%Y-%m-%d %H:%M} ({state})")
                if expiry > now and proxy["UserDN"] in info["DN"]:
                    validProxies.append(proxy)
            if not validProxies:
                problems.append("there is no valid uploaded proxy for any of the registered DNs")

    for problem in problems:
        print(f"    PROBLEM       : {problem}")
    if not problems:
        print("    No problems found in the DIRAC registry")
    print()
    return problems


@Script()
def main():
    Script.registerSwitch("", "ca=", "CA (issuer DN) of the user's certificate")
    Script.registerSwitch("n:", "limit=", "Maximum number of similar users to show (default 5)")
    Script.registerArgument(
        "user: certificate DN, DIRAC username, CERN account, email or CERN person ID", mandatory=True
    )
    Script.parseCommandLine(ignoreErrors=False)

    queryCA = None
    limit = 5
    for switch, value in Script.getUnprocessedSwitches():
        if switch == "ca":
            queryCA = normaliseDN(value)[0]
        elif switch in ("n", "limit"):
            limit = int(value)
    query = " ".join(Script.getPositionalArgs())

    users = loadUsers()
    exact, similar, queryDN = findUsers(query, users, limit)

    checkProxies = canReadProxyDB()
    if not checkProxies:
        print("NOTE: Uploaded proxies will not be checked as this requires the ProxyManagement property\n")

    problems = []
    if exact:
        print(f"Found {len(exact)} user(s) matching {query!r}:\n")
        for username in exact:
            problems += checkUser(username, users[username], queryDN, queryCA, checkProxies)
    else:
        problems.append("no exact match")
        if similar:
            print(f"No user exactly matches {query!r}, the most similar users are:\n")
            for username, score, reason in similar:
                print(f"  (score {score:.2f}: {reason})")
                if queryDN and score >= STRONG_SCORE:
                    print("  This is very likely the same person: the new certificate must be registered with the VO")
                checkUser(username, users[username], queryDN, queryCA, checkProxies)
        else:
            print(f"No user exactly matches {query!r} and no similar users were found.\n")

    if problems or not exact:
        print(
            "If the DN has changed (e.g. renewed certificate, name change or a new CA) the new\n"
            "certificate must be registered with the VO (e.g. linked to the user's account in IAM).\n"
            "Changes in the VO membership service can take a few hours to propagate to DIRAC."
        )
        dexit(1)
    print(
        "The user looks fine in the DIRAC registry. If VOMS still refuses to give an attribute certificate,\n"
        "check the certificate subject and issuer registered with the VO (e.g. in IAM)."
    )
    dexit(0)


if __name__ == "__main__":
    main()
