.. _rest_interface:

REST Interface
================

DIRAC has been extended to provide the previously described language agnostic API.  
This new API follows the *REST* style over *HTML* using *JSON* as the serialization format. 
*OAuth2* is used as the credentials delegation mechanism to the applications. All three 
technologies are widely used and have bindings already made for most of today's modern languages.  
By providing this new API DIRAC can now be interfaced to any component written in most of 
today's modern languages.

The *REST* interface enpoint is an *HTTPS* server provided in the *RESTDIRAC* module. This 
*HTTPS* server requires `Tornado <http://www.tornadoweb.org/>`_. If you don't have it installed just do::

  pip install -U "tornado>=2.4"

All requests to the *REST* API are *HTTP* requests. For more info about *REST* take a look 
`here <http://en.wikipedia.org/wiki/Representational_state_transfer>`_. From here on a basic 
understanding of the HTTP protocol is assumed.

OAuth2 authentication
-----------------------

Whenever an application wants to use the API, DIRAC needs to know on behalf of which user 
the application is making the request. Users have to grant privileges to the application so 
DIRAC knows what to do with the request. Apps have to follow a `OAuth2 <http://oauth.net/2/>`_ 
flow to get a token that has user assigned privileges. There are two different flows to get a 
token depending on the app having access to the user certificate. Both flows are one or more 
*HTTP* queries to the *REST* server.

* If the app has access to the user certificatea it has to *GET* request to */oauth2/token* using the user certificate as the client certificate. That request has to include as *GET* parameters:

  * *grant_type* set to *client_credentials*
  * *group* set to the dirac group the token is being request for.

    * To retrieve a list of valid groups for a certificate, make a *GET* request to */oauth2/groups* using the certificate.

  * *setup* set to the dirac setup the token is being request for.

    * To retrieve a list of valid setups for a certificate, make a *GET* request to */oauth2/setups* using the certificate.

      
* If the app does not have access to the user certificate (for instance a web portal) it has to:

  1. Redirect the user to */oauth2/auth* passing as *GET* parameters:

     * *response_type* set to *code*. This is a mandatory parameter.
     * *client_id* set to the identifier given yo you when the app was registered in DIRAC. This is a mandatory parameter.
     * *redirect_uri* set to the URL where the user will be redirected after the request has been authorized. Optional.
     * *state* set to any value set by the app to maintain state between the request and the callback.

  2. Once the user has authorized the request, it will be redirected to the *redirect_uri* defined either in the 
     request or in the app
     registration in DIRAC. The user request will carry the following parameters:

     * *code* set to a temporal token
     * *state* set the the original value

  3. Exchange the *code* token for the final one. Make a *GET* request to */oauth2/token* with:

     * *grant_type* set to *authorization_code*. Mandatory.
     * *code* set to the temporal token received by the client.
     * *redirect_uri* set to the original *redirect_uri* if it was defined in step 1
     * *client_id* set to the identifier. Same as in step 1.

  4. Receive access token :)

From now on. All requests to the *REST* API have to bear the access token either as:

* *GET* *access_token* parameter
* *Authorization* header with form "tokendata Bearer"

For more info check out the `OAuth2 draft <http://tools.ietf.org/html/draft-ietf-oauth-v2-31>`_.

REST API Resources
-------------------

Once the app has a valid access token, it can use the *REST* API. All data sent or received will be serialized in JSON.

Job management
***************

**GET /jobs**
  Retrieve a list of jobs matching the requirements. Parameters:

  * *allOwners*: Show jobs from all owners instead of just the current user. By default is set to *false*.
  * *maxJobs*: Maximum number of jobs to retrieve. By default is set to *100*.
  * *startJob*: Starting job for the query. By default is set to *0*.
  * Any job attribute can also be defined as a restriction in a HTTP list form. For instance::
    
     Site=DIRAC.Site.com&Site=DIRAC.Site2.com&Status=Waiting

**GET /jobs/<jid>**
  Retrieve info about job with id=*jid*


**GET /jobs/<jid>/manifest**
  Retrieve the job manifest

**GET /jobs/<jid>/inputsandbox**
  Retrieve the job input sandbox

**GET /jobs/<jid>/outputsandbox**
  Retrieve the job output sandbox

**POST /jobs**
  Submit a job. The API expects a manifest to be sent as a *JSON* object. Files can also be sent as a multipart request. 
  If files are sent, they will be added to the input sandbox and the manifest will be modified accordingly. An example 
  of manifest can be::

    {
      Executable: "/bin/echo",
      Arguments: "Hello World",
      Sites: [ "DIRAC.Site.com", "DIRAC.Site2.com" ]
    }

**DELETE /jobs/<jid>**
  Kill a job. The user has to have privileges over a job.

File catalogue
***************

All directories that have to be set in a URL have to be encoded in url safe base 64 (RFC 4648 Spec where '+' is
encoded as '-' and '/' is encoded as '_'). There are several implementations for different languages already. 

An example in python of the url safe base 64 encoding would be:

    >>> import base64
    >>> base64.urlsafe_b64encode( "/" )
    'Lw=='

Most of the search queries accept a metadata condition. This condition has to be coded as a GET query string of key value pairs. Each key
can be a metadata field and its value has to have the form 'operation|value'. The operation depends on the type of metadata field. For
integers valid operations are '<', '>', '=', '<=', '>=' and the value has to be a number. For string fields the operation has to be 'in' and
the value has to be a comma separared list of possible values. An example would be:

    someNumberField=>|4.2&someStrangeName=in|name1,name2

**GET /filecatalogue/metadata**
  Retrieve all metadata keys with their type and possible values that are compatible with the metadata restriction.
  *Accepts metadata condition*

**GET /filecatalogue/directory/<directory>**
  Retrieve contents of the specified directory. Set parameter *verbose* to true to get extended information.

**GET /filecatalogue/directory/<directory>/metadata**
  Retrieve metadata values for this directory compatible with the metadata condition.
  *Accepts metadata condition*

**GET /filecatalogue/directory/<directory>/search**
  Search from this directory subdirectories that match the requested metadata search. Each directory will also have the amount of files it contains and their total size.
  *Accepts metadata condition*
  
**GET /filecatalogue/file/<file>/attributes**
  Get the file information 

**GET /filecatalogue/file/<file>/metadata**
  Get the file metadata

  

