=====================
Web Portal User guide
=====================

  
The DIRAC Web portal is a user friendly interface allowing users to interact with the DIRAC services. 
It can be easily extended by particular VO or it can be integrated into some other portal. 


Terms:
-------

**Application** 

   A web page called application in the new portal, for example: Monitoring, Accounting, Production Management. 
   
**Desktop** 

   It is a container of different applications. Each application opens in a desktop. The desktop is your working environment. 

**State** 

   The State is the actual status of an application or a desktop. The State can be saved and it can be reused. A saved State can be shared within
   the VO or between users. 

**Theme**

   It is a graphical appearance of the web portal. DIRAC provides two themes: Desktop and Tab themes. Both themes provide similar functionalities. 
   The difference is the way of how the applications are managed. 
   The "**Desktop theme**" is similar to Microsoft Windows. It allows to work with a single desktop.
   The "**Tab theme**" is similar to web browser. Each desktop is a tab. The users can work with different desktops at the same time. 
    
Concepts:
---------

Two protocols are allowed: **http** and **https**. 
**http** protocol is very restricted. It only allows to access limited functionalities. It is recommended to the site administrators. 
The state of applications or desktops can not be saved.
**https** protocol allows to access all functionalities of DIRAC depending on your role (DIRAC group). 
The state of the application is not saved in the **URL**. The URL only contains the name of application or desktop. 
For example: `https://lhcb-portal-dirac.cern.ch/DIRAC/s:LHCb-Production/g:lhcb_prmgr/?view=tabs&theme=Grey&url_state=1|AllPlots`   

**Format of the URL**
   
   * Tab theme:
   
   Format of the URL when the Tab theme is used: 
      #. https://: protocol
      #. lhcb-portal-dirac.cern.ch/DIRAC/: host.
      #. s:LHCb-Production: DIRAC setup.
      #. g:lhcb_prmgr : role
      #. view=tabs : it is the theme. It can be **desktop** and **tabs**.
      #. theme=Grey: it is the look and feel.
      #. &url_state=1: it is desktop or application.
      #. AllPlots : it is the desktop name. the default desktop is **Default**. 
      #. The state is a desktop: AllPlots 
      #. The state is an application: *LHCbDIRAC.LHCbJobMonitor.classes.LHCbJobMonitor:AllUserJobs,*
   
   For example: desktop and application: AllPlots,*LHCbDIRAC.LHCbJobMonitor.classes.LHCbJobMonitor:AllUserJobs,* 
   
   * Desktop theme
   
    For example: `https://lhcb-portal-dirac.cern.ch/DIRAC/s:LHCb-Production/g:lhcb_prmgr/?view=desktop&theme=Grey&url_state=1|AllPlots`
      #. https://: protocol
      #. lhcb-portal-dirac.cern.ch/DIRAC/: host.
      #. s:LHCb-Production: DIRAC setup.
      #. g:lhcb_prmgr : role
      #. view=desktop : it is the theme. It can be **desktop** and **tabs**.
      #. theme=Grey: it is the look and feel.
      #. &url_state=1: it is desktop state. It can be 0 or 1.
      #. The state is a desktop: url_state=1|AllPlots  
      #. The state is an application: url_state=0|LHCbDIRAC.LHCbJobMonitor.classes.LHCbJobMonitor:statename:0:0:1440:725:0:0,0,-1,-1,-1,-1
          
  
**Note:** If you have a state saved under Desktop theme, you can open using Tab theme. This works the other way round as well.

   
A video tutorial is available at `https://www.youtube.com/watch?v=vKBpED0IyLc` link.

.. toctree::
   :maxdepth: 1

   TabTheme/index
   DesktopTheme/index
