==========================
dirac-rss-set-token
==========================

Set the token for the given element.

Usage::

  dirac-rss-set-token [option|cfgfile] <granularity> <element_name> <token> [<reason>] [<status_type>] [<duration>]

Arguments::

  granularity (string): granularity of the resource, e.g. "Site"

  element_name (string): name of the resource, e.g. "LCG.CERN.ch"

  token (string, optional): token to be assigned ( "RS_SVC" gives it back to RSS ), e.g. "ubeda"

  reason (string, optional): reason for the change, e.g. "I dont like the site admin"

  statusType ( string, optional ): defines the status type, otherwise it applies to all

  duration( integer, optional ): duration of the token.

 

 

Options::

  -g:  --Granularity=    :       Granularity of the element 

  -n:  --ElementName=    :       Name of the element 

  -k:  --Token=          :       Token of the element ( write 'RS_SVC' to give it back to RSS ) 

  -r:  --Reason=         :       Reason for the change 

  -t:  --StatusType=     :       StatusType of the element 

  -u:  --Duration=       :       Duration(hours) of the token 


