# Grafana Demo

A running example of this dashboard can be found on the LHCb grafana organisation in the Dev Area with the name `Demo Dashboard`.

## What plots does the dashboard contain?
- Production Jobs By Final Minor Status (MySQL)
- User Jobs By Final Minor Status (MySQL)
- Job CPU Efficiency By Job Type (MySQL)
- User Jobs By Job Type (ElasticSearch)
- User Jobs By Final Minor Status (MySQL)
- Pilots By Status (MySQL)

## How to import this dashboard into another Grafana installation?
- Create Grafana data sources for the MySQL database and ElasticSearch cluster (both the DIRAC certification versions).
- Create a new dashboard and in 'Dashboard settings' under 'JSON model' replace the JSON representation of the dashboard with this one.
- In the JSON file, replace the datasource uid's with the ones for the data sources in your grafana installation. You can find the UID of a data source:
    - with the [API](https://grafana.com/docs/grafana/latest/developers/http_api/data_source/)
    - by creating a new visualisation using this data source and looking into the JSON model of the visualisation (right click the title > Inspect > Panel JSON)

```
"datasource": {
        "type": "mysql",
        "uid": "R3A-SHw7z" <- CHANGE THIS VALUE TO THE ONE FOR YOUR INSTALLATION
      },
```
