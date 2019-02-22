.. _admin_dirac-admin-allow-se:

====================
dirac-admin-allow-se
====================

Enable using one or more Storage Elements

Usage::

   dirac-admin-allow-se SE1 [SE2 ...]

Options::

  -r  --AllowRead              :      Allow only reading from the storage element
  -w  --AllowWrite             :      Allow only writing to the storage element
  -k  --AllowCheck             :      Allow only check access to the storage element
  -v  --AllowRemove            :     Allow only remove access to the storage element
  -a  --All                    :     Allow all access to the storage element
  -m  --Mute                   :      Do not send email
  -S  --Site <value>           :      Allow all SEs associated to site

Example::

  $ dirac-admin-allow-se M3PEC-disk
  $
