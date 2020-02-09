Operations / VOs - Subsections
==============================

<VO_NAME> subsections allows to define pilot jobs versions for each setup defined for each VO supported by the server.

+-----------------------------------------------+----------------------------------------------+---------------------------+
| **Name**                                      | **Description**                              | **Example**               |
+-----------------------------------------------+----------------------------------------------+---------------------------+
| *<VO_NAME>*                                   | Subsection: Virtual organization name        | vo.formation.idgrilles.fr |
+-----------------------------------------------+----------------------------------------------+---------------------------+
| *<VO_NAME>/<SETUP_NAME>/*                     | Subsection: VO Setup name                    | Dirac-Production          |
+-----------------------------------------------+----------------------------------------------+---------------------------+

This section will progressively incorporate most of the other sections under /Operations in such a way 
that different values can be defined for each [VO] (in multi-VO installations) and [Setup]. A helper 
class is provided to access to these new structure.

::
  from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
  op = Operations()
  op.getSections('Dirac-Production', '')
