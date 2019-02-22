.. _admin_dirac-admin-sort-cs-sites:

=========================
dirac-admin-sort-cs-sites
=========================

Sort site names at CS in "/Resources" section. Sort can be alphabetic or by country postfix in a site name.

Usage::

  dirac-admin-sort-cs-sites [option|cfgfile] <Section>

Optional arguments::

  Section:       Name of the subsection in '/Resources/Sites/' for sort (i.e. LCG DIRAC)

Example::

  dirac-admin-sort-cs-sites -C CLOUDS DIRAC
  sort site names by country postfix in '/Resources/Sites/CLOUDS' and '/Resources/Sites/DIRAC' subsection

Options::

  -C  --country                : Sort site names by country postfix (i.e. LCG.IHEP.cn, LCG.IN2P3.fr, LCG.IHEP.su)
  -R  --reverse                : Reverse the sort order
