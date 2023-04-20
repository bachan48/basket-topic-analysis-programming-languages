package stackoverflow

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/*
Bachan Ghimire
00996378
 */

object Topics extends App {

  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("MinHash")
  val sc: SparkContext = new SparkContext(conf)
  sc.setLogLevel("ERROR") // avoid all those messages going on

  val spark = org.apache.spark.sql.SparkSession.builder().getOrCreate() //spark session init
  import spark.implicits._ //resolve dataframe implicits

  //change your file directories
//  val filename = "../DataProcessing/stackoverflow_user_languages_documents.txt"
  val filename= args(0)

  val languagesCSVReadFile = "../RawData/languages.csv"

  val dataset = spark.read.format("libsvm").load(filename)

  val languages = scala.io.Source.fromFile(languagesCSVReadFile).getLines()
  languages.next() //ignore languages file's header

  //map languages with index for reference
  val languagesMap = languages.map(lang=>{
    val language = lang.split(",")
    (language(0).toInt - 1, language(1)) //key = language index - 1 (DF loads with index - 1), value = language name
  }).toMap

  //set algorithm parameters
  val K = 25
  val seed = 0L
  val iteration = 20
  val weightThreshold = 0.05

  //run algorithm
  val lda = new LDA().setSeed(seed).setK(K).setMaxIter(iteration)
  val model = lda.fit(dataset)
  val topics = model.describeTopics()
  //topics.show(K, false)

  //final result to display
  val clusterResults = {
    topics.select("termIndices", "termWeights")
      .map(row => { //
        val indices = row.getList[Int](0)
        val weights = row.getList[Double](1)
        val clusterResult = indices zip weights //love it
        clusterResult.toMap.filter({ case (_, weight) => weight >= weightThreshold }) //discard low weight languages
      })
      .map(cluster => { //map language index to language name
        cluster.map({
          case (index, weight) => ((languagesMap(index)), weight) //get language name for language index
        })
      }).collect().toList.zipWithIndex //zip with index for cluster's index number
  }

  //print results
  clusterResults.foreach({
    case (language_weight, clusterIndex) =>
      println(s"Cluster $clusterIndex:")
      language_weight.toSeq.sortBy( - _._2).foreach({case(lang, weight)=>println(s"$lang, $weight")})
      println("\n")
  })

  sc.stop()
}