
**PREREQUISITE**

* Java
* SBT
* Spark

#Building and Running Streaming Application

 * Run the command `sbt clean pack` 
 * To run the streaming application change directory(cd) to the spark folder.(cd `/mnt/installation/spark-1.4.1-bin-hadoop2.6`) 
   - If you want to run rest server on yarn
     +  run the command `bin/spark-submit --class com.shashi.spark.streaming.StreamingMain --master yarn-client <path to rest api jar> <appname> <batchtime> <windowtime> <slidetime>`
     + path of the rest api jar will be <path to our project>/target/pack/lib/core_2.10-0.1.jar 

###Example run command

`bin/spark-submit --class com.shashi.spark.streaming.StreamingMain --packages com.datastax.spark:spark-cassandra-connector_2.10:1.5.0-M3,org.apache.spark:spark-streaming-kafka_2.10:1.5.2 --master local[2] /home/hadoop/interests/streamingapp/streamingapp/target/pack/lib/core_2.10-0.1.jar sensoranalytics 15 60 60`