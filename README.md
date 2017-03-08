# Spark Style Guide

Spark is an amazingly powerful big data engine that's written in Scala.

There are some [awesome Scala style guides](https://github.com/databricks/scala-style-guide) but they cover a lot of advanced language features that aren't frequently encountered by Spark users.  [Haters gonna hate](https://www.reddit.com/r/scala/comments/2ze443/a_good_example_of_a_scala_style_guide_by_people/)!

This guide will outline how to format code you'll frequently encounter in Spark.

## Variables

Variables should use camelCase.  Variables that point to DataFrames, Datasets, and RDDs should be suffixed accordingly to make your code readable:

* Variables pointing to DataFrames should be suffixed with `DF` (following conventions in the [Spark Programming Guide](http://spark.apache.org/docs/latest/sql-programming-guide.html))


```scala
peopleDF.createOrReplaceTempView("people")
```

* Variables pointing to Datasets should be suffixed with `DS`


```scala
val stringsDS = sqlDF.map {
  case Row(key: Int, value: String) => s"Key: $key, Value: $value"
}
```

* Variables pointing to RDDs should be suffixed with `RDD`

```scala
val peopleRDD = spark.sparkContext.textFile("examples/src/main/resources/people.txt")
```

Use the variable `col` for `Column` arguments.

```scala
def min(col: Column)
```

Use `col1` and `col2` for methods that take two `Column` arguments.

```scala
def corr(col1: Column, col2: Column)
```

Use `cols` for methods that take an arbitrary number of `Column` arguments.

```scala
def array(cols: Column*)
```

For methods that take column names, follow the same pattern and use `colName`, `colName1`, `colName2`, and `colNames` as variables.

## Chained Method Calls

Spark methods are often deeply chained and should be broken up on multiple lines.

```scala
jdbcDF.write
  .format("jdbc")
  .option("url", "jdbc:postgresql:dbserver")
  .option("dbtable", "schema.tablename")
  .option("user", "username")
  .option("password", "password")
  .save()
```

Here's an example of a well formatted extract:

```scala
val extractDF = spark.read.parquet("someS3Path")
  .select(
    "name",
    "Date of Birth"
  )
  .transform(someCustomTransformation)
  .withColumnRenamed("Date of Birth", "date_of_birth")
  .withColumnRenamed("token4", "patient_id")
  .filter(
    col("date_of_birth") > "1999-01-02"
  )
```

## Spark SQL

Use multiline strings to format SQL code:

```scala
val coolDF = spark.sql("""
select
  `first_name`,
  `last_name`,
  `hair_color`
from people
""")
```

## User Defined Functions

*Coming soon...*

## null

*Coming soon...*

## Custom transformations

Use multiple parameter lists when defining custom transformations, so you can use them with the `Dataset#transform` method.  Let's disregard this advice from the Databricks Scala style guide: "Avoid using multiple parameter lists. They complicate operator overloading, and can confuse programmers less familiar with Scala."

```scala
def withCat(name: String)(df: DataFrame): DataFrame = {
  df.withColumn("cats", lit(s"$name meow"))
}
```

The `withCat()` custom transformation can be used as follows:

```scala
val niceDf = df.transform(withCat("puffy"))
```

### Validating DataFrame dependencies

*Coming soon...*

## Whitespace

Arguments in a sequence should be separated with a space:

```scala
Seq("a", "b", "c")
```

Put one space after colons and no spaces before colons:

```scala
case class Person(name: String, age: Int)
```

There should be one space on either side of an operator (including the assignment operator):

```scala
def max(data: Array[Int]): Int = {
  var max = Int.MinValue
  for (v <- data) {
    if (v > max) {
      max = v
    }
  }
  max
}
```