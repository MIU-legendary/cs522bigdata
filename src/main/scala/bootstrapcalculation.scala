
import org.apache.spark._

import java.io._


case class Person(name: String, age: Int)

case class VocabularyPerPerson(num: String, year: String, sex: String, edu: String, vocab: String)

case class result(cato: String, mean: Double, variance: Double)

object bootstrapcalculation extends App {
  val conf = new SparkConf().setAppName("Spark Sort").setMaster("local")
  val sc = new SparkContext(conf)

  // Exploring SparkSQL
  // Initialize an SQLContext
  val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  import sqlContext._
  import sqlContext.implicits._
  import org.apache.spark.sql.functions._


  sc.setLogLevel("WARN")

  val writer = new PrintWriter(new File("result.txt"))

  writer.write("Hello World")

  var csv = sc.textFile("DATA/scala_input/Vocab.csv")

  var headerAndRows = csv.map(line => line.split(",").map(_.trim))
  val header = headerAndRows.first

  val vocalData = headerAndRows.filter(_(0) != header(0))
  val vocabulary = vocalData
    .map(c => VocabularyPerPerson(c(0), c(1), c(2), c(3), c(4)))
    .toDF

  vocabulary.printSchema()

  vocabulary.select("edu").show(5)

  val sample = vocabulary.sample(withReplacement = true,0.25)

  sample.show()

  sample.groupBy("sex").agg(avg("vocab")).show()

  val time = 0
  val pairVocab = new Pair(0,0)
  val pairEdu = new Pair(0,0)
  for (time <- 1 to 1000){
    val resample = sample.sample(withReplacement = false, 1)
    resample.
      foreach(x => {
        val todouble = x.getAs("edu").toString.toDouble
        println(todouble)
      })
  }



//
//  header.foreach(println)
//  creditCardData.first().foreach(println)


//  val res = counts.collect()
//  for (n <- res) writer.println(n.toString())
//
//  writer.close()
//
//  val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
//
//  val dfs = sqlcontext.read.json("data/Emp.json")
//
//  dfs.show()
//  dfs.printSchema()
//  val dfs1 = dfs.filter(!dfs("name").isNull)
//  dfs1.show()
//  dfs1.select("name").show()
//  dfs1.filter(dfs("age") > 23).show()
//  dfs1.groupBy("age").count().show()

}
