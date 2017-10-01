import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by pratik.joshi on 9/28/17.
  */
object Evaluator {
	def main(args: Array[String]) = {
		val conf = new SparkConf().setAppName("Language Evaluator")
		val sc = new SparkContext(conf)

		val wikiDocuments = HadoopWikiRDDGenerator.createUsing(sc, withPath = "file:///Users/pratik.joshi/Documents/Projects/SparkProject/src/main/Data/WikiPages_BigData.xml")

		val rawWikiPages = WikiCleaner.clean(wikiDocuments)

		val tokenizedWikiData = rawWikiPages.flatMap(wikiText => wikiText.split("\\W+"))

		val pertinentWikiData = tokenizedWikiData.map(wikiToken => wikiToken.replaceAll("[.|,|'|\"|?|)|(]", "").trim)
												 .filter(wikiToken => wikiToken.length > 2)

		val wikiDataSortedByLength = pertinentWikiData.distinct
														.sortBy(wikiToken => wikiToken.length, ascending = false)
														.sample(withReplacement = false, fraction = .01)

		wikiDataSortedByLength.collect.foreach(println)
	}
}
