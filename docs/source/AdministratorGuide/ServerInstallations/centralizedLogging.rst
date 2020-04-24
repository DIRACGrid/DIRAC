.. _centralized_logging:

===========================================================================
Centralized logging for DIRAC components with the ELK stack and MQ services
===========================================================================

The `ELK stack <https://www.elastic.co/elk-stack>`_ is now a de facto standard for centralizing logs. DIRAC allows you to send your Agents and Services logs to a MessageQueue system, from which the ELK stack can be fed.

We will not detail the installation of the MQ or the ELK stack here.

DIRAC Configuration
===================

First of all, you should configure a MQ in your ``Resources`` that will be used for the logging purposes. See :ref:`configuration_message_queues` for details. It would look something like::

  Resources
  {
    MQServices
    {
      messagebroker.cern.ch
      {
        MQType = Stomp
        Port = 61713
        User = myUser
        Password = myPassword
        Host = messagebroker.cern.ch
        Queues
        {
          lhcb.dirac.logging
          {
          }
        }
      }
    }
  }


Next, you should define a logging backend using the ``messageQueue`` plugin. See :ref:`gLogger_backends_messagequeue` and :ref:`gLogger_gLogger_basics_backend` for details. It would look something like::

  LogBackends
  {
    mqLogs
    {
      MsgQueue = messagebroker.cern.ch::Queues::lhcb.dirac.logging
      Plugin = messageQueue
    }
  }

Finally, you have to instruct your services or agents to use that log backend. Again, see :ref:`gLogger_gLogger_basics_backend`. For the DFC for example, it would look something like::

  FileCatalog
  {
    [...]
    LogBackends = stdout, mqLogs
  }


From the DIRAC point of view, that's all there is to do.

Logstash and ELK configurations
===============================

The logstash configuration (``/etc/logstash/conf.d/configname``) is given here as an example only (`full documentation <https://www.elastic.co/guide/en/logstash/current/configuration.html>`_)::

  input {
    # This queue is used for dirac components
    # you need one entry per broker
    # Caution, alias are not resolved into multiple hosts !
    stomp {
      type => "stomp"
      destination => "/queue/lhcb.dirac.logging"
      host => messagebroker
      port => 61713
      user => "myUser"
      password => "myPassword"
      codec => "json"
    }

  }

  filter{
    if [type] == "stomp" {
        # If there is an exception, print it multiline
        # This is the way to test if a variable is defined
        if "" in [exc_info]{
          mutate {
            gsub => [
              "exc_info", "\\n", "\n"
            ]
          }
        } else {
          # otherwise, add the field as empty string so that it does not display
          mutate {
            add_field => {"exc_info" => ""}
          }
        }
        # If levelname is not defined, we can infer that several other infos
        # are missing, like asctime. So define them empty.
        if !("" in [levelname]){
          mutate {
            add_field => {"levelname" => ""
                          "asctime" => ""}
          }
        }
        date {
          match => [ "asctime", "yyyy-MM-dd HH:mm:ss" ]
          timezone => "UTC"
        }

      # we want to create the index based on the component name
      # but the component name has a "/" in it, so replace it
      # with a "-", and set it lowercase
      # We do it in two separate mutate filter to make sure
      # of the order
      mutate {
        copy => { "componentname" => "componentindex" }
      }
      mutate {
        gsub => [
          "componentindex", "/", "-"
        ]
        lowercase => [ "componentindex" ]
      }

    }
  }

  output {
    if [type] == "stomp"  {
      elasticsearch {
          # We create one index per component per day
          index    => "lhcb-dirac-logs-%{componentindex}-%{+YYYY.MM.dd}"
          hosts    => ["https://my-elasticsearch-host.cern.ch:9203"]
          user     => "myESUser"
          template_name => "lhcb-dirac-logs_default"
          manage_template => "false"
          password => "myESPassword"
      }
    }
  }


And the ElasticSearch template ``lhcb-dirac-logs_default`` looks like::

  {
    "order": 1,
    "template": "lhcb-dirac-logs-*",
    "settings": {
      "index.number_of_shards": "1",
      "index.search.slowlog.threshold.fetch.warn": "1s",
      "index.search.slowlog.threshold.query.warn": "10s",
      "indexing.slowlog.level": "info",
      "indexing.slowlog.threshold.index.warn": "10s",
      "indexing.slowlog.threshold.index.info": "5s"
    },
    "mappings": {
      "_default_": {
        "dynamic_templates": [
          {
            "message_field": {
              "path_match": "message",
              "match_mapping_type": "string",
              "mapping": {
                "type": "text",
                "norms": false,
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              }
            }
          },
          {
            "varmessage_field": {
              "path_match": "varmessage",
              "match_mapping_type": "*",
              "mapping": {
                "type": "text",
                "norms": false,
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              }
            }
          },
          {
            "string_fields": {
              "match": "*",
              "match_mapping_type": "string",
              "mapping": {
                "type": "text",
                "norms": false,
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              }
            }
          }
        ],
        "properties": {
          "@timestamp": {
            "type": "date"
          },
          "@version": {
            "type": "keyword"
          },
          "geoip": {
            "dynamic": true,
            "properties": {
              "ip": {
                "type": "ip"
              },
              "location": {
                "type": "geo_point"
              },
              "latitude": {
                "type": "half_float"
              },
              "longitude": {
                "type": "half_float"
              }
            }
          }
        }
      }
    }
  }

Kibana dashboard
================

A dashboard for the logs can be found `here <https://github.com/DIRACGrid/DIRAC/tree/integration/dashboards/>`_