
**PREREQUISITE**

* Java
* SBT
* Spark
* Kafka
* Cassandra

#Building and Running Streaming Application

 * Run the command `sbt clean pack`
 * Setup Zepplin to use cassandra connector
 * Delete the checkpoint directory if you are updating the code
   - Download following jars
      + Cassandra connector jar `spark-cassandra-connector_2.10-1.5.0-M3.jar`
      + Cassandra client util jar `cassandra-driver-core-3.0.0-alpha4.jar`
      + Cassandra driver jar `cassandra-clientutil-2.2.2.jar` 
   - You need to add above jars into zepplin classpath and also point zepplin to the existing cassandra instance. To do so add the following into `$ZEPPELIN_HOME/conf/zeppelin-env.sh`
      + export ZEPPELIN_JAVA_OPTS="-Dspark.jars=<path to spark-cassandra-connector_2.10-1.5.0-M3.jar>:<path-to-cassandra-driver-core-3.0.0-alpha4.jar>:<path-to-cassandra-clientutil-2.2.2.jar> -Dspark.cassandra.connection.host={hostname}"
      + export ZEPPELIN_INTP_JAVA_OPTS=$ZEPPELIN_JAVA_OPTS
 * Create cassandra tables from cassandrascript

 * To run the streaming application change directory(cd) to the spark folder.(cd `/mnt/installation/spark-1.4.1-bin-hadoop2.6`) 
   - If you want to run rest server on yarn
     +  run the command `bin/spark-submit --class com.shashi.spark.streaming.StreamingMain --master yarn-client <path to rest api jar> <appname> <batchtime> <windowtime> <slidetime>`
     + path of the rest api jar will be <path to our project>/target/pack/lib/core_2.10-0.1.jar 

###Example run command

`bin/spark-submit --class com.shashi.spark.streaming.StreamingMain --packages com.datastax.spark:spark-cassandra-connector_2.10:1.5.0-M3,org.apache.spark:spark-streaming-kafka_2.10:1.5.2 --master local[2] /home/hadoop/interests/streamingapp/streamingapp/target/pack/lib/core_2.10-0.1.jar sensoranalytics 15 60 60 localhost:9092 127.0.0.1`
