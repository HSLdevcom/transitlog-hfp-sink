[![Build Status](https://travis-ci.org/HSLdevcom/transitlog-hfp-sink.svg?branch=master)](https://travis-ci.org/HSLdevcom/transitlog-hfp-sink)

## Description

Application for inserting HFP data to PostgreSQL TimescaleDB.

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

Either use released versions from public maven repository or build your own and install to local maven repository:
  - ```cd transitdata-common && mvn install```  

### Locally

- ```mvn compile```  
- ```mvn package```  

### Docker image

- Run [this script](build-image.sh) to build the Docker image


## Running

Requirements:
- Pulsar Cluster
  - By default uses localhost, override host in PULSAR_HOST if needed.
    - Tip: f.ex if running inside Docker in OSX set `PULSAR_HOST=host.docker.internal` to connect to the parent machine
  - You can use [this script](https://github.com/HSLdevcom/transitdata/blob/master/bin/pulsar/pulsar-up.sh) to launch it as Docker container
- Connection string to Transitlog database is read from file.
  - Set filepath via env variable FILEPATH_CONNECTION_STRING, default is `/run/secrets/db_conn_string`
  - Format of connection string is `jdbc:postgresql://localhost:5432/vehicles?user=hfp_writer&password=hfp_writer`

All other configuration options are configured in the [config file](src/main/resources/environment.conf)
which can also be configured externally via env variable CONFIG_PATH
