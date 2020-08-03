.. _webappdirac_developwebapp:

==============================
Developing new web application
==============================

The new DIRAC web framework provides many facilities to develop and test web applications.
This framework loads each application:

* in a separate window, and these windows can be arranged at the desktop area by means of resizing, moving and pinning.
* in a separate tab and these tabs can be customized.

In this tutorial we are going to explain the ways of developing and testing

new applications.
Before you start this tutorial, it is desirable that you have some experience with programming in Python, JavaScript, HTML,
CSS scripting, client-server communication (such as AJAX and web sockets) and sufficient knowledge
in object-oriented programming. If you are not familiar with some of the web technologies, or
there has been a while since you used those technologies, please visit the W3CSchool web site (`<http://www.w3schools.com/>`_). T
here, you can find tutorials that you can use to learn or to refresh your knowledge for web-programming.
As well we suggest to read :ref:`webappdirac_setupeclipse` section.

Each application consists of two parts:

* Client side (CS): Builds the user interface and communicates with the web server in order to get necessary data and show it appropriately.
* Server side (SS): Provides services to the client side run in browser.

The folder structure of the server side web installation is as follows:

* <Module name folder such as DIRAC, LHCbDIRAC, WebAppDIRAC>
  * **WebApp**
    * __init__.py
    * **handler**: contains all the server side implementations of the framework and all the applications.
      * __init__.py
    * **static**: contains all the static content that can be loaded by the client side such as JavaScript files, images and css files
      * <Module name folder such as DIRAC, LHCbDIRAC, WebAppDIRAC>: contains the client side implementation of each application
        * Application 1
        * Application 2
    * **template**: contains all the templates used by the files in the handler folder

In order to explain how to develop an application, we will go step by step creating an example one. We will name it **MyApp**.

Server side
-----------
Each application server side logic is implemented in one Python file. The name of the file is formed by appending the word **Handler** to the name of the application.
In the case of the application we want to build, the name of the Python file should be **MyAppHandler**.
This file has to be located into the **handler** folder.

Be aware that If this file is not defined in the folder, the application is not going to appear in the main menu.

This file defines a Python class responsible for all server side functionality of **MyApp**. The class has to
extends **WebHandler** class which is the base class for all server side applications handling clients requests.
The starting definition of this class is as follows::

.. code-block:: python

   from WebAppDIRAC.Lib.WebHandler import WebHandler

   class MyAppHandler(WebHandler):

For each type of client request there must be an entry point i.e. a method that will be invoked when a
clients' requests arrive at the server. Lets say that the URL of the requested method is **MyApp/getData**.
Therefore the name of the class is **MyAppHandler** and the name of the method within the class will be **web_getData**.
This means that if you want a method to be accessible in the application class you have to put the prefix **web_**
to the name of the method.::

.. code-block:: python

   from WebAppDIRAC.Lib.WebHandler import WebHandler

   class MyAppHandler(WebHandler):
      def web_getData(self):
         self.write({“data”:[1,2,3,4]})

In order to send back response to the client, we can use the **write** method of the **WebHandler** class. This method whenever invoked, sends to the client the value given as a parameter. If the value is of type dictionary, then the dictionary is converted to JSON string before it is sent back to the client.

The server handles all requests one-by-one which means that the server does not handle the next request until
the current one is finished. This mechanism becomes a bottleneck if one request lasts longer and increases the response time for each subsequent request waiting in the server queue until the previous one has finished. Thus the server provides a way how to asynchronously handle clients' requests and mitigate this obstacle.
Read the following link and tutorial for further information `<https://github.com/DIRACGrid/WebAppDIRAC/wiki/Asynchronous-handling-mechanisms-of-clients%27-requests>`_

Any other method that is not an entry point, can have any arbitrary name satisfying the rules of the Python programming language.

Usually the clients requests come with parameters that contain data. In order to access a parameter, you have to use the following expression::

.. code-block:: javascript

   self.request.arguments["parameter_name"][0]

or in a full example::

.. code-block:: python

   def web_ping(self):
      pingValue = self.request.arguments["ping_val"][0]
      self.write({"pong_val": pingValue})

Every parameter value is enclosed by a list by default so the 0-index stands for taking the value out of the list.

Client side
-----------

The CS side consists of files needed for rendering the UI and communicating with the server side.
Technologies used are JavaScript with ExtJS4.x, HTML and CSS. The files of the CS are located into
the **static/<Module name folder such as DIRAC, LHCbDIRAC, WebAppDIRAC>** folder and are organized as follows:

* **MyApp**: this folder is named after the name of the application we want to build. It contains all the files regarding this application.
  * **build**: this folder contains the compiled version of the javascript files contained in the classes folder
  * **classes**: this folder contains the javascript file that defines the main ExtJS class representing the application on the client side.
    * MyApp.js: this mandatory file contains the main ExtJS class representing the application on the client side. The name of the file must have the same name as the application we want to build.
  * **css**: this folder contains all the css files specific to this application.
    * MyApp.css: this mandatory file contains the css style needed by some of the components of the application. The name of the file must have the same name as the application we want to build. The file must be created no matter it contains some code or not.
  * **images**: this folder contains all the specific images and icons needed by this application.

The most important part of all files and folders is the file that contains the main ExtJS class representing the application on the client side (in our case that is MyApp.js).

This file defines a ExtJS class responsible for all client side functionality of **MyApp**. This class extends **Ext.dirac.core.Module** class which is the base class for all applications. The starting definition of this class is as follows::

.. code-block:: javascript

   Ext.define('DIRAC.MyApp.classes.MyApp', {
      extend : 'Ext.dirac.core.Module',
      requires :[]
   });

When extending the base class, there are some mandatory methods to be implemented within the derived class:
   * **initComponent**: this method is called by the constructor of the application. In this method you can set up the title of the application, its width and height, its maximized state, starting position on the screen and the icon css class. Here it is suitable to set up the layout of the entire application. For further information regarding ExtJS component layouts refer to `<http://docs.sencha.com/extjs/4.2.1/extjs-build/examples/layout-browser/layout-browser.html>`_.
   * **buildUI**: this method is used to build the user interface. Usually this is done by instantiating ExtJS widgets. These instances are added to the application in a way prescribed by the layout which is defined in the initComponent method. This method is called after all the CSS files regarding this application have been successfully loaded.
   * **getStateData**: The DIRAC web framework provides a generic way to save and load states of an application. This method is not mandatory, and it can be overridden by a new implementation in the application class. Whenever the user saves an application state, this method is called in order to take the data defining the current state of the application. The data has to be a JavaScript object.
   * **loadState(data)**: When we want to load a state, this method is being called. As an argument the framework provides the data that have been saved previously for that state.

The framework already defines handlers for some events related to the windows instances in which the applications are loaded. However there are cases when the developer would like to define some additional actions that have to be executed when those events appear.

In order to access the window object containing the instance of an application, you can use the method **getContainer()**.

For example, suppose we have an image shown inside an application. Suppose we want to resize the image
whenever the window gets resized. So the code that we need in order to support this functionality is as
follows (in the following code **this** refers to the application object)::

.. code-block:: python

      this.getContainer().__dirac_resize = function(oWindow, iWidth, iHeight, eOpts) {
              this.__oprResizeImageAccordingToWindow(image, oWindow);
      }

DIRAC reserved variables and constants
--------------------------------------

The DIRAC web framework provides a set of global variables and constants. These constants and variables can be accessed anywhere in the code.

* **GLOBAL.APP**: A reference to the main object representing the entire framework. The most important references provided by this reference are as follows:
      * **GLOBAL.APP.desktop**: A reference to the desktop object
      * **GLOBAL.APP.SM**: A reference to the state management object responsible for saving, loading, managing active state, creating and loading user interface forms related to the state management.
      * **GLOBAL.APP.CF**: A reference to the object providing common functions that can be used by applications.
* **GLOBAL.BASE_URL**: Base URL that has to be used when requesting a service from the server.
* **GLOBAL.EXTJS_VERSION**: The version of the ExtJS library
* **GLOBAL.MOUSE_X**: The X coordinate of the mouse cursor relative to the top left corner of the presentation area of the browser.
* **GLOBAL.MOUSE_Y**: The Y coordinate of the mouse cursor relative to the top left corner of the presentation area of the browser.
* **GLOBAL.IS_IE**: An indicator whether the browser embedding the system is Internet Explorer or not.
* **GLOBAL.USER_CREDENTIALS**: A reference to an object containing the user credentials.
* **GLOBAL.STATE_MANAGEMENT_ENABLED**: An indicator whether the state management is available or not.

Useful web components
---------------------

When building the client side, you can use some additional components that are not part of the standard ExtJS set of components.
These components were especially designed for the framework and the applications and can be found in **<Module name folder such
as DIRAC, LHCbDIRAC, WebAppDIRAC>/WebApp/static/core/js/utils**:

* **DiracBoxSelect**: This component looks like the standard combo-box component, but provides more functionality. Main features: supporting of multichecking, searching through the options, and making negation of the selection. You can see an example of this component within the left panel of the JobMonitor application.
* **DiracFileLoad**: Whenever you want to load an extra JavaScript file or CSS file, but also you want to define a callback upon successful loading of the file, this is the right component for doing this.
* **DiracToolButton**: This component represents a small squared button providing possibility to define menu. This button is suitable for buttons that should take small space in cases such as headers of others components. You can see an example of this component at the header of left panel of the JobMonitor.

Making MyApp application
------------------------

The application we named **MyApp** is going to present some simple functionality.
It is going to contain two visual parts: one with textarea and two buttons, and another part showing grid
with some data generated on the server. When first button gets clicked, the value of the textarea is sent
to the server and brought back to the client. When the second button gets clicked an information for a service called
by the server is shown in the textarea.

   1.First we are going to create the SS side of the **MyApp**. Go to the **[root]/handler** and create a file named **MyAppHandler.py**. This file will define the class whose instances will serve the **MyApp** client. The class will provide two services:
      * **web_getData**: this method will provide random data for the grid
      * **web_echoValue**: this method will return the same value that was sent together with the user request
      * **web_getServiceInfo**: this method will return some information about some service called from the server side. The information returned by the service is sent back to the client and shown in a textarea.

     The code::

.. code-block:: python

      from WebAppDIRAC.Lib.WebHandler import WebHandler
      from DIRAC.Core.DISET.RPCClient import RPCClient
      import random


      class MyAppHandler(WebHandler):
          """
                  The main class inherits from WebHandler
          """
          """
                  AUTH_PROPS is constant containing (a list of) properties the client
                  requesting a service has to have in order to use this class.
          """
          AUTH_PROPS = "authenticated"


          """
                  Entry-point method for data returned to the grid
          """
          def web_getData(self):
                  data = self.__generateRandomData()
                  self.write({"result": data})


          """
                  Entry-point method to echo a value sent by the client
          """
          def web_echoValue(self):
                  value = self.request.arguments["value"][0]
                  self.write({"value": value})

          """
                  Entry-point method to get service information.
                  This method presents how to asynchronously support
                  the clients requests on the server side.
          """
          @asyncGen
          def web_getServiceInfo(self):
                  RPC = RPCClient("WorkloadManagement/JobMonitoring")
                  result = yield self.threadTask(RPC.ping)
                  self.finish({"info": str(result['Value'])})

          """
                  Private method to generate random data.
                  This method cannot be called directly by the client
                  i.e. it is not an entry point
          """
          def __generateRandomData(self):
                  data = []
                  for n in range(50):
                          data.append({"value":random.randrange(1,100)})
                  return data


   2. Now we have to create the folder structure for the CS. The main folder of the **MyApp** application have
   to be located in a namespace folder. Let name that namespace folder DIRAC and place it in the **[root]/static/** folder.

      * WebApp
      * handler
      * MyAppHandler.py (already created in step 1)
      * static
         * DIRAC
            * MyApp
              * build
              * classes
              * css
              * images

   Next, the folder **MyApp** should be created in the DIRAC folder together with four new sub-folders, as mentioned in the explanation before: build, classes, css, and images folder.

   3. After we finished creating the folder structure, we have to create some mandatory files as explained before. In the [root]/static/DIRAC/MyApp/classes create the file MyApp.js file. Similarly, create the file MyApp.css in the [root]/static/DIRAC/MyApp/css folder.
   4. Open the MyApp.js. Here we have to define the main class representing the client side of the application. First we are going to code the frame of the class::

.. code-block:: javascript

         Ext.define('DIRAC.MyApp.classes.MyApp', {
            extend : 'Ext.dirac.core.Module',
            requires :[],
            initComponent:function(){},
            buildUI:function(){}
         });


  As explained before, first we have to be implement the **initComponent** and the **buildUI** methods.::

.. code-block:: javascript

      initComponent : function() {

          var me = this;

          //setting the title of the application
          me.launcher.title = "My First Application";
          //setting the maximized state
          me.launcher.maximized = false;

          //since the maximized state is set to false, we have to set the width and height of the window
          me.launcher.width = 500;
          me.launcher.height = 500;

          //setting the starting position of window, loading the application      me.launcher.x = 0;
          me.launcher.y = 0;

          //setting the main layout of this application. In this case that is the border layout
          Ext.apply(me, {
              layout : 'border',
              bodyBorder : false,
              defaults : {
                  collapsible : true,
                  split : true
              }
          });

          //at the end we call the initComponent of the parent ExtJS class
          me.callParent(arguments);

      },

      buildUI : function() {

          var me = this;

          /*
                  Creating the left panel.
                  Pay attention that the region config property is set up to west
                  which means that the panel will take the
                  left side of the available area.
          */
          me.leftPanel = new Ext.create('Ext.panel.Panel', {
              title : 'Text area',
              region : 'west',
              width : 250,
              minWidth : 230,
              maxWidth : 350,
              bodyPadding : 5,
              autoScroll : true,
              layout : {
                  type : 'vbox',
                  align : 'stretch',
                  pack : 'start'
              }
          });

          //creating the textarea
          me.textArea = new Ext.create('Ext.form.field.TextArea', {
              fieldLabel : "Value",
              labelAlign : "top",
              flex : 1
          });

          //embedding the textarea into the left panel
          me.leftPanel.add(me.textArea);

          /*
                  Creating the docked menu with a button
                  to send the value from the textarea to the server

          */

          //creating a button with a click handler
          me.btnValue = new Ext.Button({

              text : 'Echo the value',
              margin : 1,
              handler : function() {

                  Ext.Ajax.request({
                          url : GLOBAL.BASE_URL + 'MyApp/echoValue',
                          params : {
                                  value: me.textArea.getValue()
                          },
                          scope : me,
                          success : function(response) {

                                  var me = this;
                                  var response = Ext.JSON.decode(response.responseText);
                                  alert("THE VALUE: "+response.value);
                          }
                  });

              },
              scope : me
          });

          // creating a button with a click handler
          me.btnRPC = new Ext.Button({

              text : 'Service info',
              margin : 1,
              handler : function() {

                  Ext.Ajax.request({
                          url : GLOBAL.BASE_URL + 'MyApp/getServiceInfo',
                          params : {
                          },
                          scope : me,
                          success : function(response) {

                                  var me = this;
                                  var response = Ext.JSON.decode(response.responseText);
                                  me.textArea.setValue(response.info);

                          }
                  });

              },
              scope : me
          });

          //creating the toolbar and embedding the button as an item
          var oPanelToolbar = new Ext.toolbar.Toolbar({
              dock : 'bottom',
              layout : {
                  pack : 'center'
              },
              items : [me.btnValue, me.btnRPC]
          });

          /*
                  Docking the toolbar at the bottom side of the left panel
          */
          me.leftPanel.addDocked([oPanelToolbar]);

          /*
                  Creating the store for the grid
                  This object stores the data.
          */
          me.dataStore = new Ext.data.JsonStore({

              proxy : {
                  type : 'ajax',
                  url : GLOBAL.BASE_URL + 'MyApp/getData',
                  reader : {
                      type : 'json',
                      root : 'result'
                  },
                  timeout : 1800000
              },
              fields : [{
                          name : 'value',
                          type : 'int'
               }],
              autoLoad : true,
              pageSize : 50,

          });

          /*
                  Creating the grid object.
                  Pay attention that the region config property is set up to center
                  which means that the grid will take the rest of the available area.
                  Also we set the store config property to refer to the store object
                  we created previously.
          */
          me.grid = Ext.create('Ext.grid.Panel', {
              region : 'center',
              store : me.dataStore,
              header : false,
              columns : [{
                  header : 'Value',
                  sortable : true,
                  dataIndex : 'value',
                  align : 'left'
              }]
          });

          /*
                  Embedding the panel and the grid within the working area of the application
          */
          me.add([me.leftPanel,me.grid]);
      }


  5. Throughout all the code, especially in the method buildUI, there are several components created in order to structure the user interface. Therefore, you have to append all the classes used within the **DIRAC.MyApp.classes.MyApp** requires definition. In our case the list of requires would look like::

.. code-block:: javascript

         requires:   ['Ext.panel.Panel', 'Ext.form.field.TextArea', 'Ext.Button', 'Ext.toolbar.Toolbar', 'Ext.data.JsonStore', 'Ext.grid.Panel']


  6. In order to have the application within the list of applications, you have to open the **web.cfg** file
  located into the root. There you have to add new registration line within the **Schema/Applications** section::

.. code-block::

      WebApp
      {
        DevelopMode = True
        Schema
        {
          Applications
          {
            Job Monitor = DIRAC.JobMonitor
            Accounting = DIRAC.AccountingPlot
            Configuration Manager = DIRAC.ConfigurationManager
            File Catalog = DIRAC.FileCatalog
            Notepad = DIRAC.Notepad
            My First Application = DIRAC.MyApp
          }
          TestLink = link|http://google.com
        }
      }

  7. Now you can test the application. Before testing the application restart the server in order to enable the application within the main menu.

Debugging an application
------------------------

In order to debug an application, a debugging tools are needed to be used. In **Firefox** you can install and use the Firebug toolset which can be also used in **Chrome** but in a light version.

In Chrome you can use developer tools.

DIRAC web framework provides two modes of working regarding the CS. One is the development mode, which means that the JavaScripts are loaded as are, so that they can be easily debugged. The other mode is the production mode where JavaScripts are minimized and compiled before loaded. Those JavaScripts are lighter in memory but almost useless regarding the debugging process.

In order to set up the production mode, you have to set the **DevelopMode** parameter into the web.cfg file as shown as follows (by default this parameter is set to **True**)::

.. code-block::

      WebApp
      {
        DevelopMode = False

        Schema
        {
          Applications
          {
            Job Monitor = DIRAC.JobMonitor
            Accounting = DIRAC.AccountingPlot
            Configuration Manager = DIRAC.ConfigurationManager
            File Catalog = DIRAC.FileCatalog
            Notepad = DIRAC.Notepad
            My First Application = DIRAC.MyApp
          }
          TestLink = link|https://google.com
        }
      }


Before you can use the compiled version of the JavaScript files, you have to compiled them first.
For this reason you have to execute the python script **dirac-webapp-compile**.
In order to run the script, you have to download and install a tool called Sencha Cmd ( `<https://www.sencha.com/products/sencha-cmd/download>`_ ).
You can also refer to `<https://docs.sencha.com/extjs/4.2.1/#!/guide/command>`_ and read
the System Setup section for detailed installation.

Inheritance of applications
---------------------------

The inheritance of an application is done in both SS and CS. In this case let suppose that we want to inherit the **MyApp** application. Let name this new application **MyNewApp**.

The procedure for creating a new application is the same one as explained in the previous section.

When creating the python file, the Python class, namely **DIRAC.MyNewApp.classes.MyNewApp**, has to inherit from **DIRAC.MyApp.classes.MyApp**. Be aware that before you can inherit, firstly you have to import the parent file. The code would look like as follows::

.. code-block:: python

      from WebAppDIRAC.WebApp.handler.MyAppHandler import MyAppHandler
      import random

      class MyNewAppHandler(MyAppHandler):

        AUTH_PROPS = "authenticated"

When creating the main JavaScript file, in this case named **MyNewApp.js**, there are two parts
that differ from the obvious development.
First of all, the ExtJS class to be developed, namely **DIRAC.MyNewApp.classes.MyNewApp** has to extend **DIRAC.MyApp.classes.MyApp** instead of **Ext.dirac.core.Module**.

Next, when defining the buildUI method, first of all the parent buildUI has to be called before any other changes take place.

User credentials and user properties
------------------------------------

For some functionalities of the applications you have to distinguish between various kind of users.
For example, in the configuration manager, the whole configuration can be browsed, but also it can be
managed and edited. The management functionality shall be allowed only for the users that have the property of **CSAdministrator**.

On the client side, these properties of a user can be accessed via the
**GLOBAL.USER_CREDENTIALS.properties** variable. On the server side the list of user properties is
contained in **self.getSessionData().properties**.
So in the case of configuration manager, at the client side we use the following code::

   if (("properties" in GLOBAL.USER_CREDENTIALS) && (Ext.Array.indexOf(GLOBAL.USER_CREDENTIALS.properties, "CSAdministrator") != -1)) { …

At the server side of configuration manager we did a method to check whether an user is a configuration manager or not::

.. code-block:: python

   def __authorizeAction(self):
     data = SessionData().getData()
     isAuth = False
     if "properties" in data["user"]:
       if "CSAdministrator" in data["user"]["properties"]:
         isAuth = True
     return isAuth

Be aware that sometimes **properties** list is not part of the credentials object so it can be checked first for
its existence before it can be used.

Using predefined widgets
------------------------

DIRAC framework provides already implemented widgets which can be
found under (`<https://github.com/DIRACGrid/WebAppDIRAC/tree/integration/WebApp/static/core/js/utils>`_).
More details about the widgets can be found in the developer documentation:
`<https://localhost:8443/DIRAC/static/doc/index.html>`_ or in the portal (`<https://hostname/DIRAC/static/doc/index.html>`_).

Create your first example
-------------------------

We already prepared a simple example using predefined widgets
(You can found more information `<https://hostname/DIRAC/static/doc/index.html>`_ and
you can have a look the code in github: (`<https://github.com/DIRACGrid/WebAppDIRAC/tree/integration/WebApp/static/DIRAC>`_).

NOTE: Please make sure that your application will compile. You have to use::

   dirac-webapp-compile
