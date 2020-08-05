.. _admin_dirac-admin-ban-se:

==================
dirac-admin-ban-se
==================

Ban one or more Storage Elements for usage

Usage::

   dirac-admin-ban-se SE1 [SE2 ...]

Options::

  -r  --BanRead                :      Ban only reading from the storage element
  -w  --BanWrite               :      Ban writing to the storage element
  -k  --BanCheck               :      Ban check access to the storage element
  -v  --BanRemove              :     Ban remove access to the storage element
  -a  --All                    :     Ban all access to the storage element
  -m  --Mute                   :      Do not send email
  -S  --Site <value>           :      Ban all SEs associate to site (note that if writing is allowed, check is always allowed)

Example::

  $ dirac-admin-ban-se M3PEC-disk
