// Databricks notebook source
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//read the file and convert it to dataframe
val termfreq = sc.textFile("/FileStore/tables/plot_summaries.txt")
val lowerterm = termfreq.map{x=>x.toLowerCase()}.map{x=>x.split("""\W+""")}
val termDF = lowerterm.toDF()
val metadata = sc.textFile("/FileStore/tables/movie_metadata-ab497.tsv")
val dataRdd = metadata.map(line => (line.split("\t")(0),line.split("\t")(2)))

// COMMAND ----------

// Use mllib's stopwords remover to remove the stopwords 
import org.apache.spark.ml.feature.StopWordsRemover
val remover = new StopWordsRemover().setInputCol("value").setOutputCol("filtered")
val filtered_term = remover.transform(termDF).select("filtered")
val lowerterm = filtered_term.rdd.map(x=>String.valueOf(x)).map{x=>x.split("""\W+""")}.map {case w => (w(2),w.slice(3,w.length))}

// COMMAND ----------

//concat with each plot word quantity with the plot number for further processing data convieniently
val finalterm = lowerterm.map{case(id, words)=>(id.concat("_"+String.valueOf(words.size)), words)}

// COMMAND ----------

//get the wordidPair like : ((String, String), Int) = ((memory,28648635),1)
val wordidPair = finalterm.flatMap {case (id, words) => words.map {word => (word, id)}}.map{case (id, word)=> ((id,word),1)}.reduceByKey {case (n1, n2) => n1 + n2}

// COMMAND ----------

// for each word add the "plot id" +"how many times that word showed in that plot" to List for recording
val wordMap = wordidPair.map{case ((word, id), count) => (word, (id,count))}.groupBy {case (word, (id, count)) => word}.map{case (word, seq) =>val seq2 = seq map {case (_, (id, count)) => (id, count)}
        (word, seq2.mkString(", "))}

// COMMAND ----------

//For each word , count them from whole plots. Do this for tf Array[(String, Int)] = Array((zarinka,1))
val UniqueWordinPlot=wordidPair.map{x=>(x._1)}.map{x=>(x._1,1)}.reduceByKey(_+_)
val ArrayUniqueWordinPlot = UniqueWordinPlot.collect()

// COMMAND ----------

// join two table by word
val tfidf = wordMap.join(UniqueWordinPlot)

// COMMAND ----------

// calculate total number of plots
val totalPlot = lowerterm.count()

// COMMAND ----------

// Get the tfidf value for eache word
import scala.collection.mutable.ListBuffer
val final_weight = for(line <- tfidf)
yield {
  val word=line._1
  val plotCount=line._2._2
  val plots = line._2._1
  val plotList = plots.split(" ")
  val finalweight = for(plot <- plotList)
  yield {
    val movieid = plot.split(",")(0).replace("(","").replace(")","")
    val totalWord = movieid.split("_")(1)
    val wordCount = plot.split(",")(1).replace("(","").replace(")","")
    var weight=(wordCount.toDouble/totalWord.toDouble)*scala.math.log(totalPlot/plotCount.toInt+1)

    var weightList = new ListBuffer[String]()
    weightList += word
    weightList += movieid
    weightList += weight.toString
    weightList
  }
  finalweight
}

// COMMAND ----------

//Get the final tfidf table
val tfidf_table = final_weight.flatMap(x=>x).map(x=>(x(0),(x(1).split("_")(0),x(2)))).sortBy{case (word: String, (movieId: String, tfidf: String)) => -tfidf.toDouble}

// COMMAND ----------

// prepossing the /searchTerm.txt read the single term 
val searchTerms = sc.textFile("/FileStore/tables/searchTerm.txt")
val searchList = searchTerms.map( x=> x).collect()
val Oneterm = searchList(0)

// COMMAND ----------

// get the top 10 result that searched by the single term
val Topten_weights = tfidf_table.filter(x=>x._1 == Oneterm).sortBy{case (word: String, (movieId: String, tfidf: String)) => -tfidf.toDouble}.take(10)
val Topten_List = Topten_weights.map{case(word,(id,tfidf))=> (id,tfidf)}

// COMMAND ----------

val singleResult_rdd = sc.makeRDD(Topten_List)
val singleRsSet = dataRdd.join(singleResult_rdd).map{case(word,(id,tfidf))=>(id,tfidf) }.sortBy{case (name: String, tfidf: String) => -tfidf.toDouble}.collect()

// COMMAND ----------

//calculate Search term vector and save as Array[Double]
val mulitterm = searchList(1).split("""\W+""")
val termVector = for(word<-mulitterm)
yield{    
      val total = mulitterm.size
      val numList = ArrayUniqueWordinPlot.filter(x => x._1 == word).map(x=> x._2)
      if(ArrayUniqueWordinPlot.map(x=>x._1).contains(word)){
      val idf = scala.math.log(totalPlot.toDouble/(numList(0)+1))
      val tf = 1/total.toDouble
      val weight = tf * idf
      weight
      }else{
      val idf =0
      val tf = 0
      val weight = tf * idf
      weight
      }
} 

// COMMAND ----------

// The function for calculating the cosine similarity
def cosineSimilarity(x: Array[Double], y: Array[Double]): Double = {
    require(x.size == y.size)
    dotProduct(x, y)/(magnitude(x) * magnitude(y))
    
}
def dotProduct(x: Array[Double], y: Array[Double]): Double = {
    (for((a, b) <- x zip y) yield a * b) sum
}
def magnitude(x: Array[Double]): Double = {
    math.sqrt(x map(i => i*i) sum)
}

// COMMAND ----------

// get the whole Movie ID List
val movieIDList = lowerterm.map(x=>x._1).collect()

// COMMAND ----------

// each time process one movie, filter out that movie
// see if there exists the search term word tf-idf , if so , get the value
// else set the value is zero 
// compute vector of termVector * movieIdVector 
// save that value as (movieId,cosineSimilar)
val cosineSimilar_table = for(line <- movieIDList)
yield{
  val movieId =  line
  val findtable = tfidf_table.filter(x => x._2._1 == movieId).collect()
  val tfidfForTerm = for(word<-mulitterm)
  yield{
      if(findtable.map(x=>x._1).contains(word)){
          val numlist = findtable.filter(x=>x._1== word).map(x=> x._2._1)
          val value = numlist(0).toDouble
          value
      }else{
          val value = 0.0
          value
      }
  }
  val cosValue = cosineSimilarity(termVector,tfidfForTerm )
  if(cosValue.isNaN){
    var result = new ListBuffer[String]()
    result +=0.0.toString
    result +=movieId.toString
    result 
  }else{
    var result = new ListBuffer[String]()
    result +=cosValue.toString
    result +=movieId.toString
    result
  }
}

// COMMAND ----------

val finaltable = cosineSimilar_table.map(x=>(x(1),x(0))).sortBy{case (cosine: String, movieId: String) => -cosine.toDouble}

// COMMAND ----------

val Table_rdd = sc.makeRDD(finaltable)

// COMMAND ----------

val result = dataRdd.join(Table_rdd)
result.map{case(id,(name,cosine)) => (name,cosine)}.sortBy{case (name: String,cosine: String ) => -cosine.toDouble}.collect()
