# Grafana Demo

## Data sources used
- MySQL Accounting database for DIRAC certification
- ElasticSearch cluster for DIRAC certification

## What plots does the dashboard contain?
- Production Jobs By Final Minor Status
- User Jobs By Final Minor Status
- Job CPU Efficiency By Job Type
- User Jobs By Job Type
- User Jobs By Final Minor Stattus
- Pilots By Status

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
