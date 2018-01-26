name := "dhals"

version := "1.0"

scalaVersion := "2.11.1"

resolvers += Resolver.mavenLocal

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.0-SNAPSHOT"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.3.0-SNAPSHOT" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.0-SNAPSHOT" % "test"
libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0" % "test"
        