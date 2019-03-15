===================
Technology Previews
===================


When new technologies are introduced within DIRAC, there are cases when we allow this technology not to be used.
The reason might be that it is not completely mature yet, or that it can be disturbing. These technologies are toggled by either CS flags or environment variables.
They are meant to stay optional for a couple of releases, and then they become the default.
This page keeps a list of such technologies.

M2Crypto
========

We aim at replacing the home made wrapper of openssl pyGSI with the standard M2Crypto library. It is by default disabled.
You can enable it by setting the environment variable `DIRAC_USE_M2CRYPTO` to `Yes`.
