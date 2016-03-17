# spanstore-elasticsearch

This is an Elasticsearch 2 SpanStore. The SpanStore utilizies the Elasticsearch Java client
library with a node client for optimal performance.

This spanstore is not included in the zipkin-java binary by default. To use it, add it to the
classpath by setting the `LOADER_PATH environment` variable, also making sure to set STORAGE_TYPE
and any other elasticsearch options you need.
An example invocation may be

```
$ LOADER_PATH=spanstore-elasticsearch-0.8.1.jar STORAGE_TYPE=elasticsearch
  ES_HOSTS=ES1.github.com:9300,ES2.github.com:9300 ES_CLUSTER_NAME=monitoring
  java -jar zipkin-server-0.8.1-exec.jar
```

`zipkin.elasticsearch.ElasticsearchProperties` includes defaults that will operate
against a local Cassandra installation.

