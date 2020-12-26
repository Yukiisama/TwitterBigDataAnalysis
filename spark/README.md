# how to launch

mvn package
cd spark_maven
spark-submit target/TPSpark-0.0.1.jar --deploy-mode cluster

maybe put others options on spark submit cmd for hbase etc...