name := course.value + "-" + assignment.value

scalaVersion := "2.11.12"

scalacOptions ++= Seq("-deprecation")

courseId := "e8VseYIYEeWxQQoymFg8zQ"

resolvers += Resolver.sonatypeRepo("releases")

// grading libraries
libraryDependencies += "junit" % "junit" % "4.10" % Test
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test
)

// include the common dir
commonSourcePackages += "common"
