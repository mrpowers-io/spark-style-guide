// most runtimes (Databricks, Spark shell) import implicits by default, so you don't need to run this
import spark.implicits._

val peopleDF = Seq(
  ("bob", 44),
  ("cindy", 10)
).toDF("first_name", "age")

// view the DataFrame
peopleDF.show()

//+----------+---+
//|first_name|age|
//+----------+---+
//|       bob| 44|
//|     cindy| 10|
//+----------+---+

// suffix DataFrames with DF
peopleDF.createOrReplaceTempView("people")

// once you create the view, you can query it with SQL
spark.sql("select * from people where age > 20").show()

//+----------+---+
//|first_name|age|
//+----------+---+
//|       bob| 44|
//+----------+---+



