import sbt._

object Dependencies {
  val resolutionRepos = Seq(    
    "Maven Repository" at "http://mvnrepository.com/artifact/"
  )

  object V {
    val sparkV="1.5.2"
    val cassandraConnectorV="1.5.0-M3"
  }

  object Libraries {   
    // Spark
    val sparkCore = "org.apache.spark" %% "spark-core" % V.sparkV
    val sparkSql = "org.apache.spark" %% "spark-sql" % V.sparkV
    val sparkStreaming = "org.apache.spark" %% "spark-streaming" % V.sparkV
    val sparkHive = "org.apache.spark" %% "spark-hive" % V.sparkV
    val sparkKafka = "org.apache.spark" %% "spark-streaming-kafka" %V.sparkV
    val cassandraConnector = "com.datastax.spark" % "spark-cassandra-connector_2.10" % V.cassandraConnectorV
  }

}
