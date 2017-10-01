
# Apache Spark

####WordCounter
This is the basic application in the Big Data world - it's the "hello world" of Big Data

####Evaluator
This is the code to read a set of Wikipedia pages related to Big Data and perform some operations on them

Learnings and Notes:
1. Place the `build.sbt` file at the same level as the top-level project
2. Place the `assembly.sbt` file in the `<Project-Name>/project directory`

Run:
1. Start the `sbt` console by typing `sbt` in the terminal
2. Run the `clean` and `assembly` commands in the sbt console to generate a fresh new uber jar
3. Run the `spark-submit` command to run the job, as -
````
spark-submit --class "Evaluator" --master "local[*]" /Users/pratik.joshi/Documents/Projects/SparkProject/target/scala-2.11/SparkProject-1.0.jar
````