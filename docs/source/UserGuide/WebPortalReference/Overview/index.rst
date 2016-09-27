=========================
Overview
=========================

DIRAC Web Portal is a Web application which provides access to all the aspects of the DIRAC distributed computing system. It allows to monitor and control all the activities in a natural desktop application like way. In order to reach this goal DIRAC Web Portal is built using GUI elements to mimic desktop applications, such as toolbars, menus, windows buttons and so on.


Description
========================

All pages have two toolbars, one on the top and another at the bottom of the pages that contain the main navigation widgets. The top toolbar contains the main menu and reflects the logical structure of the Portal. It also allows to select active DIRAC setup. The bottom toolbar allows users to select their active group and displays the identity the user is connected with.

The mostly used layout within our Web Portal is a table on the right side of the page and a side bar on the left. Almost all data that needs to be displayed can be represented as two-dimensional matrix using a table widget. This widget has a built-in pagination mechanism and is very customizable. As a drawback, it is a bit slow to load the data into the table. On an average desktop hardware, tables with more than 100 elements can be slow to display the data.



.. figure:: DIRAC-portal-overview.jpg 

    DIRAC Web Portal

1. **Main Menu**: This menu offers options for systems, jobs, tools and help.
2. **Selections**: Shows a set of selectors than permits generate customs selections.
3. **Buttons to open/collapse panels**: Permit open or collapse left menu.
4. **Actions to perform for job(s)**: These actions permits select all, select none, reset, kill or submit
5. **Menu to change DIRAC setup**: Users can change between different setups.
6. **Current location**: Indicates where the user is located inside the portal.
7. **Buttons to submit or reset the form**: After options are selected its possible to submit and execute the selection or reset the selectors.
8. **Pagination controls**: Permits navigate between the pages, and also show in which page the user is navigating.
9. **Refresh table**: Reload the page without loose the previous selection and show the new status.
10. **Items per page**: This option allow the users to specify how many items are going to be displayed by page.
11. **User DIRAC login**: Login assigned to the user connected to DIRAC web portal.
12. **DIRAC Group**: The user could belong to different groups and perform actions depending of the group previously selected.
13. **Certificate DN**: Web portal shows the distinguish name of user certificate what is being used to realize the connection.
14. **Index items displayed**: Display the range of items displayed in the page.

Note: Some options are not displayed in all Web Portal pages, as selections.


Functionalities
========================

DIRAC Web Portal is a Web based User Interface than provide several actions depending of each group and privileges of the user into DIRAC. Actions by user privileges are showed below:

-   **Users**: Track jobs and data, perform actions on jobs as killing or deleting.
-   **Production Managers**: Can define and follow large data productions and react if necessary starting or stopping them.
-   **Data Managers**: Allows to define and monitor file transfer activity as well as check requests set by jobs.
-   **Administrators**: Can manage, browse, watch logs from servers.