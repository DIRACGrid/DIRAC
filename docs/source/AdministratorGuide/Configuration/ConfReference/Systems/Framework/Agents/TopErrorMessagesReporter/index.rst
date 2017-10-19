Systems / Framework / <INSTANCE> / Agents / TopErrorMessagesReportes - Sub-subsection
=====================================================================================

TopErrorMessagesReporter produces a list with the most common errors injected in the SystemLoggingDB and sends a
notification to a mailing list and specific users.


The attributes of this agent are showed in the table below:

+------------------+---------------------------------------+---------------------------+
| **Name**         | **Description**                       | **Example**               |
+------------------+---------------------------------------+---------------------------+
| *MailList*       | List of DIRAC users than the reporter | MailList = mseco@in2p3.fr |
|                  | going to receive Top Error Messages   |                           |
+------------------+---------------------------------------+---------------------------+
| *NumberOfErrors* | Number of top errors to be reported   | NumberOfErrors = 10       |
+------------------+---------------------------------------+---------------------------+
| *QueryPeriod*    | Each how many time the agent is going | QueryPeriod = 7           |
|                  | to make the query, expressed in days  |                           |
+------------------+---------------------------------------+---------------------------+
| *Reviewer*       | Login of DIRAC user in charge of      | Reviewer = mseco          |
|                  | review the error message monitor      |                           |
+------------------+---------------------------------------+---------------------------+
| *Threshold*      |                                       | Threshold = 10            |
+------------------+---------------------------------------+---------------------------+
