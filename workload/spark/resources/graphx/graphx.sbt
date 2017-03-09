name := "graphx app"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.6.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.2.0"

libraryDependencies += "graphframes" % "graphframes" % "0.3.0-spark1.6-s_2.10"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.1"

resolvers +=Resolver.mavenLocal

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Spark Repository additional" at "https://dl.bintray.com/spark-packages/maven/"
