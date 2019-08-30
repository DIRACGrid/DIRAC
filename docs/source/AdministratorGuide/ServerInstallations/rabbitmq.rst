========
RabbitMQ
========


RabbitMQ Administration Tools
------------------------------

RabbitMQ uses a two-step access-control(https://www.rabbitmq.com/access-control.html). Apart
from the standard user/password (or ssl-based) authentication, RabbitMQ has an internal database
with the list of users and permissions settings.
DIRAC provides an interface to the internal RabbitMQ user database via the RabbitMQAdmin class.
Internally it uses rabbitmqctl command (https://www.rabbitmq.com/man/rabbitmqctl.1.man.html) 
Only the user with the granted permissions can execute those commands.
The interface provides methods for  adding or removing users, setting the permission etc.
The interface do not provide the possibilty to e.g. create or destroy queues, because according
to the AMPQ and general RabbitMQ philosophy those operations should be done by consumers/producer
with given permissions. 


Synchronization of RabbitMQ user database
-----------------------------------------

The synchronization between the DIRAC Configuration System and the RabbitMQ internal
database is assured by RabbitMQSynchronizer.
It checks the current list of users and hosts which are allowed to send messages to 
RabbitMQ and updates the internal RabbitMQ database accordingly.
