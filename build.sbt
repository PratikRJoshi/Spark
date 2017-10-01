name := "SparkProject"

version := "1.0"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq("org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided",
							"org.apache.hadoop" % "hadoop-streaming" % "2.7.3")

assemblyJarName in assembly := s"${name.value.replace(' ', '-')}-${version.value}.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)