Systems / Framework / <INSTANCE> / Service / Notification - Sub-subsection
==========================================================================

The Notification service provides a toolkit to contact people via email
(eventually SMS etc.) to trigger some actions.

The original motivation for this is due to some sites restricting the
sending of email but it is useful for e.g. crash reports to get to their
destination.

Another use-case is for users to request an email notification for the
completion of their jobs.  When output data files are uploaded to the
Grid, an email could be sent by default with the metadata of the file.
    
It can also be used to set alarms to be promptly forwarded to those
subscribing to them. 


Extra options required to configure the Notification system are:

+-------------+----------------------------------+---------------------------+
| **Name**    | **Description**                  | **Example**               |
+-------------+----------------------------------+---------------------------+
| *SMSSwitch* | SMS switch used to send messages | SMSSwithc = sms.switch.ch |
+-------------+----------------------------------+---------------------------+
