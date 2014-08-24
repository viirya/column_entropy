name := "Calculate column entropy"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.0.2"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.0.2"
 
resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

