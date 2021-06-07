
import org.apache.spark._
import org.apache.spark

import java.io._


case class Person(name: String, age: Int)

case class VocabularyPerPerson(num: String, year: Int, sex: String, edu: Int, vocab: Int)

case class result(edu: Int, mean: Double, variance: Double)

case class ResampleDF(edu: Int, vocab: Int)

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

  writer.println("Spark Scala Project")

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
//  vocabulary.show(10)

  val meanEdu = vocabulary.groupBy("edu")
    .agg(avg("vocab").alias("Mean"),
      variance("vocab").alias("Variance"))
    .orderBy("edu")
    .withColumnRenamed("edu","Category")


  writer.println("STEP 3 RESULT")
  meanEdu.show()
  meanEdu.foreach(x => writer.println(x.toString()
    .replace("[","")
    .replace("]","")))




  // Step 4 get sample of dataset with no replacement and fraction is 25%
  val sample = vocabulary.sample(withReplacement = false,0.25).cache()

  // Create blank DF with schema result
  var resultDF = Seq.empty[result].toDF()

  // Step 5 get resample for many time
  for (time <- 1 to 10){
    val resample = sample.sample(withReplacement = true, 1)
    val resampleDF = resample
      .map(c =>ResampleDF(c(3).toString.toInt
        ,c(4).toString.toInt))
      .toDF

    val calculate = resampleDF.groupBy("edu")
      .agg(avg("vocab").alias("mean"),
      variance("vocab").alias("variance"))
    // Add resampleDF to resultDF
    resultDF = resultDF.unionAll(calculate)
  }

  val finalResult = resultDF.groupBy("edu")
    .agg(avg("mean").alias("MeanAvg"),
      avg("variance").alias("VarianceAvg"))
    .orderBy("edu")
    .withColumnRenamed("edu","Category")


  writer.println("STEP 5 RESULT")
  finalResult.foreach(x => writer.println(x.toString()
    .replace("[","")
    .replace("]","")))

  finalResult.show()

  writer.close()

}
