package listart

import org.apache.spark.{SparkConf, SparkContext}


/**
 * Inverted index
 * word => {(path,wordCount),...}
 */
object InvertedIndexApp {
  def main(args: Array[String]): Unit = {
    val dataFile = "distcp-spark/input/data"
    val output = "distcp-spark/output/output"
    val conf = new SparkConf().setMaster("local").setAppName("Inverted Index")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile(dataFile)
      .map(_.split(". ",2))
      .flatMap(line => line(1)
        .replaceAll("^\"|\"$", "")
        .split(" ")
        .map((line(0), _))) // (path, word)
      .map((_, 1))  // (path, word) => ((path, word), 1)
      .reduceByKey(_+_)
      .map({case ((p, w), n) => (w, (p, n))}) // ((path, word), num) => (word, (path, num))
      .groupByKey()
      .sortByKey()
      .map({case (w, seq) => "\"" + w + "\":\t" + seq.toList.sorted.mkString("{", ", ", "}")})

    rdd.saveAsTextFile(output)

    println(rdd.collect() mkString "\n")
  }
}
