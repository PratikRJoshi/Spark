import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.SparkContext

/**
  * Created by pratik.joshi on 9/30/17.
  */
object HadoopWikiRDDGenerator {
	def createUsing(sc : SparkContext, withPath: String) = {
		val jobConf = new JobConf()
		jobConf.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader")
		jobConf.set("stream.recordreader.begin", "<page>")
		jobConf.set("stream.recordreader.end", "</page>")

		FileInputFormat.addInputPaths(jobConf, withPath)

		sc.hadoopRDD(jobConf, classOf[org.apache.hadoop.streaming.StreamInputFormat],
			classOf[Text], classOf[Text])	}
}
