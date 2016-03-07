import sbt._

object Dependencies {
  val resolutionRepos = Seq(    
    "Maven Repository" at "http://mvnrepository.com/artifact/"
  )

  object V {
    val sparkV="1.5.2"
  }

  object Libraries {   
    // Spark
    val sparkCore = "org.apache.spark" %% "spark-core" % V.sparkV
    val sparkSql = "org.apache.spark" %% "spark-sql" % V.sparkV
    val sparkStreaming = "org.apache.spark" %% "spark-streaming" % V.sparkV
    val sparkHive = "org.apache.spark" %% "spark-hive" % V.sparkV
  }

}
