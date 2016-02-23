# whisper-migrator
A tool for migrating data from Graphite Whisper files to InfluxDB TSM files (version 0.10.0).

This tool can be used in three modes

1. Get whisper file information. This option displays, number of points in the file
  and oldest timestamp in the file

  migration.go -wspinfo -wspPath=whisper folder

2. Write to influxdb using go client, clientv2
  It uses influxdb go client, clientv2. And migrates data calling HTTP APIs.
  This option can be invoked as

   migration.go -option=ClientV2 -wspPath=whisper folder -from=<2015-11-01> -until=<2015-12-30> -dbname=migrated
     -host=http://localhost -port=8086, -retentionPolicy=default -tagconfig=config.json

3. Write to influxdb using TSMWriter
   This option, uses TSMWriter and creates .tsm file directly in the influxData folder.
   This option will write the graphite data faster than the option 1
   This option can be invoked as follows

    migration.go -option=TSMW -wspPath=whisper folder -influxDataDir=influx data folder -from=<2015-11-01> -until=<2015-12-30>
      -dbname=migrated -retentionPolicy=default -tagconfig=config.json

    The influxd daemon process must be restarted to see the migrated data.

Tag Config file

  This file is required to specify tags and measurement name for a given pattern. Please see the sample tagconfig file, migration_config.json
