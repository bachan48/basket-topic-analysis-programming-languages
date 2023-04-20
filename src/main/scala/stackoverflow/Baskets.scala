package stackoverflow

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}

/*
Bachan Ghimire
00996378
 */

object Baskets extends App {

  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("MinHash")
  val sc: SparkContext = new SparkContext(conf)
  sc.setLogLevel("ERROR") // avoid all those messages going on

  val spark = org.apache.spark.sql.SparkSession.builder().getOrCreate() //spark session init
  import spark.implicits._ //resolve dataframe implicits

//  val filename = "../DataProcessing/stackoverflow_user_languages_baskets.csv"
  val filename = "C:/Users/piani/Downloads/shopify/reviews_tokenized.csv"
//  val filename = args(0)

  //read csv file
  import java.nio.charset.{Charset, CharsetDecoder, CodingErrorAction}
  val decoder: CharsetDecoder = Charset.forName("UTF-8").newDecoder
  val decoder2 = decoder.onMalformedInput(CodingErrorAction.IGNORE)
  val csvData = scala.io.Source.fromFile(filename)(decoder2).getLines()
  csvData.next() //ignore headers

  //load languages as baskets
  val baskets= csvData.map(x=>x.split(",").tail.distinct).toArray
//  val baskets= csvData.map(x=>x.split(",").map(x=>cleanString(x)).tail).toArray

  //set algorithm parameters
  val minSupport = 0.02
  val minConfidence = 0.5
//  val itemsColumnName = "languages"
  val itemsColumnName = "topics"

  val FPGrowth = new FPGrowth()
    .setItemsCol(itemsColumnName)
    .setMinSupport(minSupport)
    .setMinConfidence(minConfidence)

  val dataFrame = spark.createDataset(baskets).toDF(itemsColumnName) //create dataframe from baskets

  val frequentItemSets = FPGrowth.fit(dataFrame).freqItemsets //generate frequent item sets

  val frequentLanguages = { //prepare output
    frequentItemSets.select(col(colName = "freq"), col(colName = "items")).rdd
      .filter(row => row.getSeq[String](1).length > 1) //keep language frequency pair > 1
      .map(row => {
        val frequency = row.getLong(0)
        val languages = row.getSeq[String](1).sorted.toList //sort by language
        (frequency, languages)
      }).collect().toList
      .sortBy( - _._1) //sort by frequency descending
  }

  //print results
  println("Frequency, Languages: \n")
  frequentLanguages.foreach({
    case (frequency, languages) =>
      println(s"$frequency, ${languages.mkString(",")}")
  })

  //in case some CSV input comes wrapped with "" or [] around them
  def cleanString(input: String): String = {
    input.replace("\"", "")
      .replace("[", "")
      .replace("]", "")
      .trim
  }

  sc.stop()
}
