.. _tornado_components_logs:

===============================
Split Tornado logs by component
===============================

Dirac offers the ability to write logs for each component. One can find logs in <DIRAC FOLDER>/startup/<DIRAC COMPONENT>/log/current

In case of Tornado, logs come from many components, and can be hard to sort.

Using Fluent-bit will allow to collect logs from files, rearrange content, then send them elsewhere like an ELK instance or simply other files.
Thus, in case of ELK, it's now possible to monitor and display informations through Kibana and Grafana tools, using filters to sort logs, or simply read other splitted log files, one by component.

The idea behind that is to deal with logs independantly from Dirac. It is also possible to grab servers metrics such as cpu, memory and disk usage, giving the opportunity to make correlations between logs and server usage.

DIRAC Configuration
-------------------

First of all, you should configure a JSON Log Backend in your ``Resources`` and ``Operations`` like::

  Resources
  {
    LogBackends
    {
      StdoutJson
      {
        Plugin = StdoutJson
      }
    }
  }

  Operations
  {
    Defaults
    {
      Logging
      {
        DefaultBackends = StdoutJson
      }
    }
  }


Fluent-bit Installation
-----------------------

On each Dirac server, install Fluent-bit (https://docs.fluentbit.io)::

  curl https://raw.githubusercontent.com/fluent/fluent-bit/master/install.sh | sh

Fluent-bit Configuration
------------------------

Edit and add in /etc/fluent-bit/fluent-bit.conf::

  @INCLUDE dirac-json.conf

Create following files in /etc/fluent-bit

dirac-json.conf (Add all needed components and choose the output you want)::

  [SERVICE]
    flush             1
    log_level         info
    parsers_file      dirac-parsers.conf

  [INPUT]
      name              cpu
      tag               metric
      Interval_Sec      10

  [INPUT]
      name              mem
      tag               metric
      Interval_Sec      10

  [INPUT]
      name              disk
      tag               metric
      Interval_Sec      10

  [INPUT]
      name              tail
      parser            dirac_parser_json
      path              <DIRAC FOLDER>/startup/<DIRAC_COMPONENT>/log/current
      Tag               log.<DIRAC_COMPONENT>.log
      Mem_Buf_Limit     50MB

  [INPUT]
      name              tail
      parser            dirac_parser_json
      path              <DIRAC FOLDER>/startup/<ANOTHER_DIRAC_COMPONENT>/log/current
      Tag               log.<ANOTHER_DIRAC_COMPONENT>.log
      Mem_Buf_Limit     50MB

  [FILTER]
      Name              modify
      Match             log.*
      Rename            log message
      Add               levelname DEV

  [FILTER]
      Name              modify
      Match             *
      Add               hostname ${HOSTNAME}

  [FILTER]
      Name              Lua
      Match             log.*
      script            dirac.lua
      call              add_raw

  [FILTER]
      Name              rewrite_tag
      Match             log.tornado
      Rule              $tornadoComponent .$ $TAG.$tornadoComponentclean.log false
      Emitter_Name      re_emitted

  #[OUTPUT]
  #    name             stdout
  #    match            *

  [OUTPUT]
      Name              file
      Match             log.*
      Path              /vo/dirac/logs
      Mkdir             true
      Format            template
      Template          {raw}

  [OUTPUT]
      name                es
      host                <host>
      port                <port>
      logstash_format     true
      logstash_prefix     <index prefix>
      tls                 on
      tls.verify          off
      tls.ca_file         <path_to_ca_file>
      tls.crt_file        <path_to_crt_file>
      tls.key_file        <path_to_key_file>
      match               log.*

  [OUTPUT]
      name                es
      host                <host>
      port                <port>
      logstash_format     true
      logstash_prefix     <index prefix>
      tls                 on
      tls.verify          off
      tls.ca_file         <path_to_ca_file>
      tls.crt_file        <path_to_crt_file>
      tls.key_file        <path_to_key_file>
      match               metric

``dirac-json.conf`` is the main file, it defines different steps such as::
  [SERVICE] where we describe our json parser (from dirac Json log backend)
  [INPUT] where we describe dirac components log file and the way it will be parsed (json)
  [FILTER] where we apply modifications to parsed data, for example adding a levelname "DEV" whenever logs are not well formatted, typically "print" in code, or adding fields like hostname to know from which host logs are coming, but also more complex treatments like in dirac.lua script (described later)
  [OUTPUT] where we describe formatted logs destination, here, we have stdout, files on disks and elasticsearch.

dirac-parsers.conf::

  [PARSER]
    Name         dirac_parser_json
    Format       json
    Time_Key     asctime
    Time_Format  %Y-%m-%d %H:%M:%S,%L
    Time_Keep    On

``dirac-parsers.conf`` describes the source format that will be parsed, and the time that will be used (here asctime field) as reference

dirac.lua::

  function add_raw(tag, timestamp, record)
    new_record = record

    if record["asctime"] ~= nil then
        raw = record["asctime"] .. " [" .. record["levelname"] .. "] [" .. record["componentname"] .. "] "
        if record["tornadoComponent"] ~= nil then
            patterns = {"/"}
            str = record["tornadoComponent"]
            for i,v in ipairs(patterns) do
                str = string.gsub(str, v, "_")
            end
            new_record["tornadoComponentclean"] = str
            raw = raw .. "[" .. record["tornadoComponent"] .. "] "
        else
            raw = raw .. "[]"
        end
        raw = raw .. "[" .. record["customname"] .. "] " .. record["message"] .. " " .. record["varmessage"] .. " [" .. record["hostname"] .. "]"
        new_record["raw"] = raw
      else
        new_record["raw"] = os.date("%Y-%m-%d %H:%M:%S %Z") .. " [" .. record["levelname"] .. "] " .. record["message"] .. " [" .. record["hostname"] .. "]"
      end

      return 2, timestamp, new_record
  end

``dirac.lua`` is the most important transformation we perform on primarily logs, it builds new record depending on logs containing or not special field tornadocomponent, then cleans and formats it before sending to the outputs.

Testing
-------

Before throwing logs to ElasticSearch, config can be tested in Standard output by uncommenting::

  [OUTPUT]
      name             stdout
      match            *

...and commenting ElasticSearch outputs.

Then by using command::

  /opt/fluent-bit/bin/fluent-bit -c //etc/fluent-bit/fluent-bit.conf

NOTE: When all is OK, uncomment ElasticSearch outputs and comment stdout output

Service
-------

``sudo systemctl start/stop fluent-bit.service``

Dashboards
----------

In case of logs sent to an ELK instance, dashboards are available `here <https://github.com/DIRACGrid/DIRAC/tree/integration/dashboards/tornadoLogs>`_.

On disk
-------

In case of logs sent to local files, Logrotate is mandatory.

Having a week log retention, Logrotate config file should look like
/etc/logrotate.d/diraclogs::

  /vo/dirac/logs/* {
    rotate 7
    daily
    missingok
    notifempty
    compress
    delaycompress
    create 0644 diracsgm dirac
    sharedscripts
    postrotate
        /bin/kill -HUP `cat /var/run/syslogd.pid 2>/dev/null` 2>/dev/null || true
    endscript
  }

along with crontab line like

``0 0 * * * logrotate /etc/logrotate.d/diraclogs``
