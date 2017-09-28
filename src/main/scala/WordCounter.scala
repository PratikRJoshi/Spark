import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by pratik.joshi on 9/27/17.
  */
object WordCounter {
	def main(args: Array[String]) {
		val file = "README.md"
		val conf = new SparkConf().setAppName("Word Counter")
		val sc = new SparkContext(conf)
		val textFile = sc.textFile(file)
		val tokenizedFileData = textFile.flatMap(line => line.split(" "))
		val countPrep = tokenizedFileData.map(word => (word, 1))
		val counts = countPrep.reduceByKey((accumValue, newValue) => accumValue + newValue)
		val sortedCounts = counts.sortBy(kvPair => kvPair._2, false)
		sortedCounts.saveAsTextFile("ReadmeCountViaCodeOutput")
	}
}
