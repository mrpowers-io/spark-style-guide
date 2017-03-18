# Spark Style Guide

Spark is an amazingly powerful big data engine that's written in Scala.

There are some [awesome Scala style guides](https://github.com/databricks/scala-style-guide) but they cover a lot of advanced language features that aren't frequently encountered by Spark users.  [Haters gonna hate](https://www.reddit.com/r/scala/comments/2ze443/a_good_example_of_a_scala_style_guide_by_people/)!

This guide will outline how to format code you'll frequently encounter in Spark.

## <a name='TOC'>Table of Contents</a>

  1. [Variables](#variables)
  1. [Chained Method Calls](#chained-method-calls)
  1. [Spark SQL](#spark-sql)
  2. [Code Organization](#code-organization)
  2. [User Defined Functions](#user-defined-functions)
  3. [Custom Transformations](#custom-transformations)
  4. [JAR Files](#jar-files)
  5. [Testing](#testing)

## <a name='variables'>Variables</a>

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

## <a name='chained-method-calls'>Chained Method Calls</a>

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
  .filter(
    col("date_of_birth") > "1999-01-02"
  )
```

## <a name='spark-sql'>Spark SQL</a>

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


## <a name='code-organization'>Code Organization</a>

Here is how to organize code (from most preferred to least preferred location):

1. Open source user defined functions
2. Private user defined functions
3. Custom DataFrame transformations

Open source user defined functions are generic and can be reused in a variety of contexts.  The [`org.apache.spark.sql.functions`](https://spark.apache.org/docs/2.1.0/api/java/org/apache/spark/sql/functions.html) class provides some great examples.

Functions that contain proprietary information and cannot be made open source should be organized as user defined functions in private repositories.

Custom DataFrame transformations rely on the schema of the underlying DataFrame and should be avoided when possible.  You'll often be forced to choose between a custom DataFrame transformation and a user defined function that takes several arguments.  Requiring users to pass multiple arguments to a function can make code harder to use and that's when you'll need to fall back to custom DataFrame transformations.

## <a name='user-defined-functions'>User Defined Functions</a>

*Coming soon...*

## <a name='null'>null</a>

`null` should be used in DataFrames for values that are [unknown, missing, or irrelevant](https://medium.com/@mrpowers/dealing-with-null-in-spark-cfdbb12f231e#.fk27ontik).

Spark core functions frequently return `null` and your code can also add `null` to DataFrames (by returning `None` or explicitly returning `null`).

In general, it's better to keep all `null` references out of code and use `Option[T]` instead.  `Option` is a bit slower and explicit `null` references may be required for performance sensitve code.  Start with `Option` and only use explicit `null` references if `Option` becomes a performance bottleneck.

The schema for a column should set nullable to `false` if the column should not take `null` values.

## <a name='custom-transformations'>Custom transformations</a>

Use multiple parameter lists when defining custom transformations, so you can chain your custom transformations with the `Dataset#transform` method.  Let's disregard this advice from the Databricks Scala style guide: "Avoid using multiple parameter lists. They complicate operator overloading, and can confuse programmers less familiar with Scala."

You need to use multiple parameter lists to write awesome code like this:

```scala
def withCat(name: String)(df: DataFrame): DataFrame = {
  df.withColumn("cats", lit(s"$name meow"))
}
```

The `withCat()` custom transformation can be used as follows:

```scala
val niceDF = df.transform(withCat("puffy"))
```

### Validating DataFrame dependencies

DataFrame transformations that make schema assumptions should error out if the assumed DataFrame columns aren't present.

Suppose the following `fullName` transformation assumes the `df` has `first_name` and `last_name` columns.

```scala
val peopleDF = df.transform(fullName)
```

If the DataFrame doesn't contain the required columns, it should error out with a readable error message:

com.github.mrpowers.spark.daria.sql.MissingDataFrameColumnsException: The [first\_name] columns are not included in the DataFrame with the following columns [last\_name, age, height].

See the [spark-daria](https://github.com/MrPowers/spark-daria) project for a `DataFrameValidator` class that makes it easy to validate the presence of columns in a DataFrame.

## <a name='jar-files'>JAR Files</a>

JAR files should be named like this:

```
spark-testing-base_2.11-2.1.0_0.6.0.jar
```

Generically:

```
spark-testing-base_scalaVersion-sparkVersion_projectVersion.jar
```

If you're using sbt assembly, you can use the following line of code to build a JAR file using the correct naming conventions.

```scala
assemblyJarName in assembly := s"${name.value}_2.11-${sparkVersion.value}_${version.value}.jar"
```

*TODO* Figure out a better way to include the Scala version than hardcoding it

## <a name='testing'>Testing</a>

Use the [spark-testing-base](https://github.com/holdenk/spark-testing-base) library for writing tests with Spark.

Test the `#and` instance method defined in the [functions class](https://spark.apache.org/docs/2.1.0/api/java/org/apache/spark/sql/functions.html) as follows:

```scala
class FunctionsSpec extends FunSpec with DataFrameSuiteBase {

  import spark.implicits._
	
  describe("#and") {
	
    it ("returns true if both columns are true") {
	
      // some code
	
    }
    
  }
	
}
```

Test static methods as follows:

```scala
describe(".standardizeName") {

  it("consistenly formats the name") {
  
    // some code
    
  }
  
}
```

Instance methods are preceded with a pound sign (e.g. `#and`) and static methods are preceded with a period (e.g. `.standardizeName`) in the `describe` block.  This follows [Ruby testing conventions](http://betterspecs.org/#describe).