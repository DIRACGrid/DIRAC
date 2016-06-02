==========================
DISET Stable connections
==========================

*DISET* is the communication, authorization and authentication framework of top of which DIRAC services are built. Traditionally *DISET*
offered *RPC* and file transfer capabilities. Those communication mechanisms are not well suited for the Executor framework. *RPC* doesn't
allow the server to send data to the clients asynchronously, and each *RPC* query requires establishing a new connection and going through another *SSL* handshake. 
On average the *SSL* process is the most resource consuming part of the request. 

.. figure:: Messages.png
   :width: 450px
   :alt: stable connections diagram
   :align: right


The *Executor framework* relies on a new *DISET* capability. Support for stable connections and asynchronous requests has been added.
Any component can open a connection and reuse it to send and receive requests though it. Services can send information to clients without
having to wait for the clients to ask for them as shown in the stable connections figure.

Although once connected services can send data asynchronously to clients, services are still servers and require clients to start the
connection to them. **No service can start the connection towards the client**. Once the service has received the connection the asynchonous
data transfer can take place.

Server side usage
===========================

Any *DIRAC* service can make use of the stable connection mechanism. It's usage is quite similar to the usual *RPC* mechanism but with
extended capabilities. Here we have an example of a service using the stable connections mechanism:

.. literalinclude:: service.py
   :language: python
   :linenos:

The first thing the server requires is a definition of the messages that it can use. In the example, lines 7 and 8 define two messages:
*Ping* and *Pong* messages. Each message has one attribute called *id* that can only be either an integer or a long.  Lines 10-22 define the
connection callback *conn_connected*. Whenever the client receives a new client connection this function will be called. This function
receives three parameters:

:trid: Transport identifier. Each client connection will have a unique id. If a client reconnects it will have a different *trid* each time.
:identity: Client identifier. Each client will have a unique id. This id will be maintained across reconnects.
:kwargs: Dictionary containing keyword arguments sent by client when connecting.

If this function doesn't return *S_OK* the client connection will be rejected.

If a client drops the connection, method *conn_drop* will be called with the *trid* of the disconnected client to allow the handler to clean
up it's state regarding that client if necessary.

Lines 32-46 define callback for *Ping* message. All message callbacks will receive only one parameter. The parameter will be an object
containing the message data. As seen in line 37, the message object will have defined the attributes previously defined with the values the
client is sending. Accessing them is as easy as just accessing normal attributes. On line 38 the *Pong* message is created and then assigned
a value in to the *id* attribute on line 43. Finally the message is sent back to the client using *srv_msgSend* with the client *trid* as
first parameter and the *Pong* message as second one. To just reply to a client there's a shortcut function *srv_msgReply*. If any message
callback doesn't return *S_OK* the client will be disconnected.

In the example there's no callback for the *Pong* message because not all services may have to react to all messages. Some messages will
only make sense to be sent to clients not received from them. If the Service receives the *Pong* message, it will send an error back to the
client and disconnect it.

Client side usage
=======================

Clients do not have to define which messages they can use. The Message client will automatically discover those based on the service to
which they are connecting. Here's an example on how a client could look like:

.. literalinclude:: client.py
   :language: python
   :linenos:

Let's start with like 39 onwards. The client app is instancing a *MessageClient* pointing to the desired service. After that it registers
all the callbacks it needs. One for receiving *Pong* messages and one for reacting to disconnects. After that it just connects to the
server and sends the first *Ping* message. Lastly it will just wait 10 seconds before exiting.

Function *sendPingMsg* in line 5 onwards just creates a *Ping* message and sends it to the server via the supplied *msgClient*. 

The *pongCB* function will be executed for each *Pong* message received. Messages received on the client callbacks have a special attribute
*msgClient* with the client that has received the message. If this attribute is accessed in services it will just return *None*. 

Function *disconnectedCB* will be invoked if the client is disconnected from the service. In the example it will just try to reconnect for
some time and then exit if it doesn't manage to do so.




