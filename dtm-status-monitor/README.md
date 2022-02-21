# Status monitor
An obligatory supporting service to the Prostore main service `dtm-query-execution-core` that
actually subscribes on `__consumer_offsets` and provides information about status for specified `group.id` and `topic`

## Useful links
[Documentation (Rus)](https://datamarts.github.io/docs_prostore/getting_started/getting_started.html)

## Local deployment
The cloning and building of this service is the part of the [Prostore deployment](../README.md).
The resulting jar file is located in the `dtm-status-monitor/target` folder.

### Service configuration
The status-monitor configuration file is located in the `dtm-status-monitor/src/main/resources` folder.
The status-monitor service looks for the configuration in the same subfolder (target) where `dtm-status-monitor-<version>.jar` is executed.
So we create the respective symbolic link
```shell script
sudo ln -s ~/prostore/dtm-status-monitor/src/main/resources/application.yml ~/prostore/dtm-status-monitor/target/application.yml
```

### Run application
#### Run status-monitor service as a single jar
```shell script
# execute dtm-status-monitor with the port, specified in the Prostore configuration (core:kafka:statusMonitor)
cd ~/prostore/dtm-status-monitor/target
java -Dserver.port=9095 -jar dtm-status-monitor-<version>.jar
# attempting to run with no port specified will lead to the 8080 port competition with the Prostore main service
```

## Request/Response formats

### Request
```json
{
	"consumerGroup": "tarantool-group-csv",
	"topic": "AK_ACCOUNTS_EXT"
}
```

### Response
```json
{
  "topic": "AK_ACCOUNTS_EXT",
  "consumerGroup": "tarantool-group-csv",
  "consumerOffset": 3,
  "producerOffset": 3,
  "lastCommitTime": 1598280542139
}
```

### Use example
```shell script
curl --request POST \
  --url http://127.0.0.1:9095/status \
  --header 'content-type: application/json' \
  --data '{
	"consumerGroup": "tarantool-group-csv",
	"topic": "AK_ACCOUNTS_EXT"
}'
```
