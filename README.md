# Spark Style Guide

Spark is an amazingly powerful big data engine that's written in Scala.

There are some [awesome Scala style guides](https://github.com/databricks/scala-style-guide) but they cover a lot of advanced language features that aren't frequently encountered by Spark users.  [Haters gonna hate](https://www.reddit.com/r/scala/comments/2ze443/a_good_example_of_a_scala_style_guide_by_people/)!

This guide will outline how to format stuff you'll frequently encounter in Spark.

## Variables

Variables should use camelCase.  Variables that point to DataFrames, Datasets, and RDDs should be suffixed accordingly to make your code readable:

* DataFrames should be suffixed with `DF` (following conventions in the [Spark Programming Guide](http://spark.apache.org/docs/latest/sql-programming-guide.html))

* Datasets should be suffixed with `DS`

* RDDs should be suffixed with `RDD`

Here are some examples from the Spark Programming Guide:

```scala
peopleDF.createOrReplaceTempView("people")

val stringsDS = sqlDF.map {
  case Row(key: Int, value: String) => s"Key: $key, Value: $value"
}

val peopleRDD = spark.sparkContext.textFile("examples/src/main/resources/people.txt")
```

## Chained Method Calls


