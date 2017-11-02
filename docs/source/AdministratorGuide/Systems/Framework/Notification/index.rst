.. _framework_notification:


The Framework/Notification service
==================================


The Framework/Notification service is responsible for notification, like as send mail, sms or alarm window on DIRAC portal.
Send an email with supplied body to the specified address using the Mail utility.
If avoidSpam is True, then emails are first added to a set so that duplicates are removed, 
and sent every hour.


Configure
---------


The Notification service have next SMTP configuration parameters::

 Systems
 {
   Framework
   {
     Notification
     {
       SMTP
       {
         Port = < port of smtp server >
         Host = < smtp host name >
         Login = < account on smtp >
         Password = ***
         Protocol = < smtp protocol SSL/TSL (default None) >
       }
     }
   }
 }