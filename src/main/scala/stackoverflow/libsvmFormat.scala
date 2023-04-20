package stackoverflow

import scala.annotation.tailrec

/*
Read basket.csv file to create libsvm.txt file.
Reference of language index from languages.csv as a Map.
Bachan Ghimire
00996378
 */

object libsvmFormat extends App {

  import java.io._   //import java.io._ to create write buffer

  //change to you read/write your directory
  val languagesCSVReadFile = "../RawData/languages.csv"
  val basketsCSVReadFile = "../DataProcessing/stackoverflow_user_languages_baskets.csv"
  val libsvmWriteFile = "../DataProcessing/stackoverflow_user_languages_documents.txt"

  val languages = scala.io.Source.fromFile(languagesCSVReadFile).getLines()
  languages.next() //ignore header

  //map languages with index for reference
  val languagesMap = languages.map(lang=>{
    val language = lang.split(",")
    (language(1), language(0).toInt) //key = language name, value = index, may seem counter-intuitive!
  }).toMap

  val baskets =   scala.io.Source.fromFile(basketsCSVReadFile).getLines()
  baskets.next() //ignore header

  val writeLines = baskets.map(record=>{

    val basket = record.split(",")
    val userid = cleanString(basket.head)
    val languages = basket.map(item=>cleanString(item)).tail
    val indexes = languages.map(item=>languagesMap(item)).toList.sorted //sort indexes for libsvm
    val languageLine = libsvmFormatter(indexes,"")

    s"$userid $languageLine" //return basket line with user in libsvm format
  }).toList

  //change your write directory
  val writeFile = new File(libsvmWriteFile)
  val writer = new BufferedWriter(new FileWriter(writeFile))
  for (line <- writeLines){
    writer.write(line)
    writer.write("\n") //add new line
  }
  writer.close()

  @tailrec
  def libsvmFormatter(indexes:List[Int], accumulator: String): String ={
    if(indexes.nonEmpty){
      val result = accumulator + s"${indexes.head.toString}:1 "
      libsvmFormatter(indexes.tail, result)
    }
    else accumulator //return final string when no more languages left for a user
  }

  def cleanString(input: String): String = {
    input.replace("\"", "")
      .replace("[", "")
      .replace("]", "")
      .trim
  }

}