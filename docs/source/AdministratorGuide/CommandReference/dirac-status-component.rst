.. _admin_dirac-status-component:

======================
dirac-status-component
======================

Status of DIRAC components using runsvstat utility

Usage::

  dirac-status-component [option|cfgfile] ... [system [service|agent]]

Arguments::

  system:        Name of the system for the component (default *: all)
  service|agent: Name of the particular component (default *: all)

Example::

  $ dirac-status-component
  DIRAC Root Path = /vo/dirac/versions/Lyon-HEAD-1296215324
                                           Name : Runit    Uptime    PID
            WorkloadManagement_PilotStatusAgent : Run        4029     1697
             WorkloadManagement_JobHistoryAgent : Run        4029     1679
                        Framework_CAUpdateAgent : Run        4029     1658
                      Framework_SecurityLogging : Run        4025     2111
                     WorkloadManagement_Matcher : Run        4029     1692
             WorkloadManagement_StalledJobAgent : Run        4029     1704
            WorkloadManagement_JobCleaningAgent : Run        4029     1676
                                     Web_paster : Run        4029     1683
             WorkloadManagement_MightyOptimizer : Run        4029     1695
               WorkloadManagement_JobMonitoring : Run        4025     2133
       WorkloadManagement_StatesAccountingAgent : Run        4029     1691
               RequestManagement_RequestManager : Run        4025     2141
                     DataManagement_FileCatalog : Run        4024     2236
                  WorkloadManagement_JobManager : Run        4024     2245
           WorkloadManagement_TaskQueueDirector : Run        4029     1693
                         Framework_Notification : Run        4026     2101
                                      Web_httpd : Run        4029     1681
                         Framework_ProxyManager : Run        4024     2260
                           Framework_Monitoring : Run        4027     1948
            WorkloadManagement_WMSAdministrator : Run        4027     1926
              WorkloadManagement_InputDataAgent : Run        4029     1687
                        Framework_SystemLogging : Run        4025     2129
                           Accounting_DataStore : Run        4025     2162
                  Framework_SystemAdministrator : Run        4026     2053
                     Accounting_ReportGenerator : Run        4026     2048
               Framework_SystemLoggingDBCleaner : Run        4029     1667
             DataManagement_StorageElementProxy : Run        4024     2217
                             Framework_Plotting : Run        4025     2208
                           Configuration_Server : Run        4029     1653
                WorkloadManagement_SandboxStore : Run        4025     2186
                   Framework_UserProfileManager : Run        4025     2182
                  DataManagement_StorageElement : Run        4024     2227
             Framework_TopErrorMessagesReporter : Run        4029     1672
                       Configuration_CE2CSAgent : Run           1    32461
              WorkloadManagement_JobStateUpdate : Run        4025     2117
                  Framework_SystemLoggingReport : Run        4024     2220
                       Framework_BundleDelivery : Run        4025     2157
