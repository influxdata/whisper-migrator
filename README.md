# whisper-migrator
A tool for migrating data from Graphite Whisper files to InfluxDB TSM files (version 0.10.0).

This tool can be used in two modes

1. Write to influxdb using go client, clientv2
  It uses influxdb go client, clientv2. And migrates data calling HTTP APIs.
  This option can be invoked as 

   migration.go -option=ClientV2 -wspPath=whisper folder -from=<2015-11-01> -until=<2015-12-30> -dbname=migrated 
     -host=http://localhost -port=8086, -retentionPolicy=default -tagconfig=config.json

2. Write to influxdb using TSMWriter
   This option, uses TSMWriter and creates .tsm file directly in the influxData folder. This option will write the
   graphite data faster than the option 1
   This option can be invoked as follows

    migration.go -option=TSMW -wspPath=whisper folder -influxDataDir=influx data folder -from=<2015-11-01> -until=<2015-12-30>
      -dbname=migrated -retentionPolicy=default -tagconfig=config.json

Tag Config file

  This file is required to specify tags and measurement name for a given pattern. Please see the sample tagconfig file, migration_config.json
