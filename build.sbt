name := "Scala"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.8",
    "org.apache.spark" % "spark-core_2.12" % "3.0.1",
    "org.apache.spark" %% "spark-sql" % "3.0.1",
    "io.circe" %% "circe-core" % "0.14.0",
    "io.circe" %% "circe-generic" % "0.14.0",
    "io.circe" %% "circe-parser" % "0.14.0"
)


//libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
//libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "3.0.1" % Provided
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"  % Provided