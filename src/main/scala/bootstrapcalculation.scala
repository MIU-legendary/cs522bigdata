
import org.apache.spark._
import org.apache.spark

import java.io._


case class Person(name: String, age: Int)

case class VocabularyPerPerson(num: String, year: Int, sex: String, edu: Int, vocab: Int)

case class result(edu: Int, vocab: Int)

object bootstrapcalculation extends App {
  val conf = new SparkConf().setAppName("Spark Sort").setMaster("local")
  val sc = new SparkContext(conf)

  // Exploring SparkSQL
  // Initialize an SQLContext
  val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  import org.apache.spark.sql.functions._
  import sqlContext.implicits._

  sc.setLogLevel("WARN")

  val writer = new PrintWriter(new File("result.txt"))

  writer.write("Hello World")

  var csv = sc.textFile("DATA/scala_input/Vocab.csv")

  var headerAndRows = csv.map(line => line.split(",").map(_.trim))
  val header = headerAndRows.first

  val vocalData = headerAndRows.filter(_(0) != header(0))
  val vocabulary = vocalData
    .map(c => VocabularyPerPerson(
      c(0).toString,
      c(1).toString.toInt,
      c(2).toString,
      c(3).toString.toInt,
      c(4).toString.toInt))
    .toDF

  vocabulary.printSchema()

  vocabulary.select("edu").show(5)


  val meanEdu = vocabulary.groupBy("edu")
    .agg(avg("vocab").alias("Mean"),
      variance("vocab").alias("Variance"))
    .orderBy("edu")
    .withColumnRenamed("edu","Category")


  println("STEPPPP 3")
  meanEdu.show()

  // Step 4 get sample of dataset with no replacement and fraction is 25%
  val sample = vocabulary.sample(withReplacement = false,0.25)


  // Create blank DF with schema result
  var resultDF = Seq.empty[result].toDF()


  // Step 5 get resample for many time
  for (time <- 1 to 10){
    val resample = sample.sample(withReplacement = true, 1)
    val resampleDF = resample
      .map(c =>result(c(3).toString.toInt
        ,c(4).toString.toInt))
      .toDF

    // Add resampleDF to resultDF
    resultDF = resultDF.union(resampleDF)
  }

  val step5Result = resultDF.groupBy("edu")
    .agg(avg("vocab").alias("Mean"),
      variance("vocab").alias("Variance"))
    .orderBy("edu")
    .withColumnRenamed("edu","Category")


  println("STEPPP 5")
  step5Result.show()



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
