.. _pilots-with-tokens:

=====================================
Submitting pilots to CEs using tokens
=====================================


This guide outlines the process of setting up DIRAC to submit pilots using access tokens obtained via a ``client_credentials`` flow from a token provider.

Setting up an ``IdProvider``
----------------------------

- Set up an OAuth2 client in the token provider and obtain a ``client_id`` and a ``client_secret``.

    .. warning:: The client credentials obtained are confidential, store them in a secure place.
       Any malicious user able to get access to them would be able to generate access tokens on your behalf.
       To avoid any major issue, we recommend you to only grant essential privileges to the client (``compute`` scopes).

- Add the client credentials in the ``dirac.cfg`` of the relevant server configuration such as:

    .. code-block:: guess

      Resources
      {
        IdProviders
        {
          <IdProvider name>
          {
            client_id = <client_id>
            client_secret = <client_secret>
          }
        }
      }

- Then in your global configuration, add the following section to set up an ``IdProvider`` interface:

    .. code-block:: guess

      Resources
      {
        IdProviders
        {
          <IdProvider name>
          {
            issuer = <OIDC provider issuer URL>
          }
        }
      }

- Finally, connect the OIDC provider to a specific VO by adding the following option:

  .. code-block:: guess

    Registry
    {
      VO
      {
        <VO name>
        {
          IdProvider = <IdProvider name>
        }
      }
    }

.. note:: Get more details about the DIRAC configuration from the :ref:`Configuration <dirac-configuration>` section.

Launching the ``TokenManagerHandler``
-------------------------------------

Install the ``Framework/TokenManager`` Tornado service, following :ref:`these instructions <httpsTornadoInstall>`

.. note:: the ``Tornado/Tornado`` service might need to be restarted.

Marking computing resources and VOs as token-ready
--------------------------------------------------

To specify that a given VO is ready to use tokens on a given CE, add the ``Tag = Token:<VO>`` option within the CE section, and then restart the ``Site Directors``.
Once all your VOs are ready to use tokens, just specify ``Tag = Token``.
