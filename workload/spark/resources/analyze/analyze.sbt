name := "analyze"
version := "1.0"
scalaVersion := "2.10.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.1"
resolvers += Resolver.mavenLocal
resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
resolvers += "Spark Repository additional" at "https://dl.bintray.com/spark-packages/maven/"
