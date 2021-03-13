// snippets from this doc can be copy / pasted into different Spark runtimes

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

// name the Column arguments with the col1, col2 convention
def fullName(col1: Column, col2: Column): Column = concat(col1, lit(" "), col2)

val namesDF = Seq(
  ("bob", "loblaw"),
  ("cindy", "crawford")
).toDF("first_name", "last_name")

namesDF
  .withColumn("full_name", fullName($"first_name", $"last_name"))
  .show()

//+----------+---------+--------------+
//|first_name|last_name|     full_name|
//+----------+---------+--------------+
//|       bob|   loblaw|    bob loblaw|
//|     cindy| crawford|cindy crawford|
//+----------+---------+--------------+



