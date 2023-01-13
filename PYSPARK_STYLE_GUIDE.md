# PySpark Style Guide

PySpark provides you with access to Python language bindings to the an amazingly powerful Apache Spark big data engine.

This document draws on PySpark source code to outline coding conventions and best practices.

Comprehensive Python style guides / code formatting tools already exist and this document focuses specifically on the style issues for PySpark programmers.

As with any style guide, take caution:

> Any style guide written in English is either so brief that it’s ambiguous, or so long that no one reads it.
> - [Bob Nystrom](http://journal.stuffwithstuff.com/2015/09/08/the-hardest-program-ive-ever-written/)

## <a name='imports'>Imports</a>

Import the Pyspark SQL functions into a variable named `F` to avoid polluting the global namespace.

```python
from pyspark.sql import functions as F
```

For lesser user libraries, you can import the namespace, so subsequent invocations in the codebase are readable.  Here's an example with the [quinn library](https://github.com/MrPowers/quinn):

```python
import quinn

...

quinn.validate_absence_of_columns(source_df, ["age", "cool"])
```

This import style makes it easy to identify where the `validate_absence_of_columns` function was defined.

You can also use this import style:

```python
from quinn import validate_absence_of_columns
```

## Automatic code formatting

You should use [Black](https://github.com/psf/black) to automatically format your code in a PEP 8 compliant manner.

You should use automatic code formatting for both your projects and your notebooks.

## Variable naming conventions

Variables should use `snake_case`.  Variables that point to DataFrames, Datasets, and RDDs should be suffixed to make your code readable:

* Variables pointing to DataFrames should be suffixed with `df`:

```scala
people_df.createOrReplaceTempView("people")
```

* Variables pointing to RDDs should be suffixed with `rdd`:

```scala
val people_rdd = spark.sparkContext.textFile("examples/src/main/resources/people.txt")
```

Use the variable `col` for `Column` arguments.

```scala
def min(col: Column)
```

Use `col1` and `col2` for functions that take two `Column` arguments.

```scala
def corr(col1: Column, col2: Column)
```

Use `cols` for functions that take an arbitrary number of `Column` arguments.

```scala
def array(cols: Column*)
```

Use `col_name` for functions that take a `String` argument that refers to the name of a `Column`.

```scala
def sqrt(col_name: String): Column
```

Use `col_name1` and `col_name2` for methods that take multiple column name arguments.

Collections should use plural variable names.

```scala
animals = ["dog", "cat", "goose"]

// DON'T DO THIS
animal_list = ["dog", "cat", "goose"]
```

Singular nouns should be used for single objects.

```scala
my_car_color = "red"
```

## Columns

Columns have name, type, nullable, and metadata properties.

Columns that contain boolean values should use predicate names like `is_nice_person` or `has_red_hair`.  Use `snake_case` for column names, so it's easier to write SQL code.

You can write `(col("is_summer") && col("is_europe"))` instead of `(col("is_summer") === true && col("is_europe") === true)`.  The predicate column names make the concise syntax readable.

Columns should be typed properly.  Don't overuse `StringType` columns.

Columns should only be nullable if `null` values are allowed.  Code written for nullable columns should always handle `null` values gracefully.

Use acronyms when needed to keep column names short.  Define any acronyms used at the top of the data file, so other programmers can follow along.

Use the following shorthand notation for columns that perform comparisons.

* `gt`: greater than
* `lt`: less than
* `leq`: less than or equal to
* `geq`: greater than or equal to
* `eq`: equal to
* `between`: between two values

Here are some example column names:

* `player_age_gt_20`
* `player_age_gt_15_leq_30`
* `player_age_between_13_19`
* `player_age_eq_45`

## Custom Transformations

Here's an example of a custom transformat that returns `child` when the age is less than 13, `teenager` when the age is between 13 and 18, and `adult` when the age is above 18.

```scala
import pyspark.sql.Column

def life_stage(col):
  when(col < 13, "child").when(col >= 13 && col <= 18, "teenager").when(col > 18, "adult")
```

The `life_stage()` function will return `null` when `col` is `null`.  All built-in Spark functions gracefully handle the `null` case, so we don't need to write explicit `null` logic in the `life_stage()` function.

Custom SQL functions can also be optimized by the Spark compiler, so this is a good way to write code.  Read [this blog post](https://mungingdata.com/pyspark/chaining-dataframe-transformations/) for a full discussion on custom transformation functions.

## Chaining Custom Transformations

TODO

## User defined functions

You can write User Defined Functions (UDFs) when you need to write code that leverages Python programming features / Python libraries that aren't accessible in Spark.

Here's an example of a UDF that downcases and removes the whitespace of a string:


```scala
def betterLowerRemoveAllWhitespace(s: String):
  val str = Option(s).getOrElse(return None)
  Some(str.toLowerCase().replaceAll("\\s", ""))

val betterLowerRemoveAllWhitespaceUDF = udf[Option[String], String](betterLowerRemoveAllWhitespace)
```

The `betterLowerRemoveAllWhitespace()` function explicitly handles `None` input, so the function won't error out with a `NullPointerException`.  You should always write UDFs that handle `None` input gracefully.

In this case, a custom SQL function can provide the same functionality, but with less code:

```scala
def bestLowerRemoveAllWhitespace()(col: Column): Column = {
  lower(regexp_replace(col, "\\s+", ""))
}
```

UDFs [are a black box](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-udfs-blackbox.html) from the Spark compiler's perspective and should be avoided whenever possible.

Most logic can be coded as a custom SQL function.  Only revert to UDFs when the native Spark API isn't sufficient.

See [this blog post](TODO) for more information about UDFs.

## Function naming conventions

* `with` precedes transformations that add columns:

  - `withCoolCat()` adds the column `cool_cat` to a DataFrame

  - `withIsNicePerson()` adds the column `is_nice_person` to a DataFrame.

* `filter` precedes transformations that remove rows:

  - `filterNegativeGrowthRate()` removes the data rows where the `growth_rate` column is negative

  - `filterInvalidZipCodes()` removes the data with a malformed `zip_code`

* `enrich` precedes transformations that clobber columns.  Clobbing columns should be avoided when possible, so `enrich` transformations should only be used in rare circumstances.

* `explode` precedes transformations that add rows to a DataFrame by "exploding" a row into multiple rows.

## Schema Dependent DataFrame Transformations

Schema dependent DataFrame transformations make assumptions about the underlying DataFrame schema.  Schema dependent DataFrame transformations should explicitly validate DataFrame dependencies to clarify intentions of the code and provide readable error messages.

The following `with_full_name()` DataFrame transformation assumes the underlying DataFrame has `first_name` and `last_name` columns.

```scala
def with_full_name(df):
  df.withColumn(
    "full_name",
    F.concat_ws(" ", col("first_name"), col("last_name"))
  )
```

You should use [quinn](https://github.com/mrpowers/quinn) to validate the schema requirements of a DataFrame transformation.

```scala
import quinn

def with_full_name()(df: DataFrame): DataFrame = {
  quinn.validatePresenceOfColumns(df, Seq("first_name", "last_name"))
  df.withColumn(
    "full_name",
    F.concat_ws(" ", col("first_name"), col("last_name"))
  )
}
```

Notice how the refactored function makes it clear that this function requires `first_name` and `last_name` columns to run properly.

You can also refactor the custom transformation to remove the column name dependency:

```scala
def with_full_name(first_name_col, last_name_col):
  quinn.validatePresenceOfColumns(df, Seq(firstColName, lastColName))
  df.withColumn(
    "full_name",
    F.concat_ws(" ", col(firstColName), col(lastColName))
  )
```

Schema Independent DataFrame Transformations

Schema independent DataFrame transformations do not depend on the underlying DataFrame schema, as discussed in [this blog post](https://medium.com/@mrpowers/schema-independent-dataframe-transformations-d6b36e12dca6).

```scala
def withAgePlusOne(
  ageColName: String,
  resultColName: String
)(df: DataFrame): DataFrame = {
  df.withColumn(resultColName, col(ageColName) + 1)
}
```

The `withAgePlusOne` allows users to pass in column names, so the function can be applied to DataFrames with different schemas.

Schema independent DataFrame transformations also allow for column validations, so they output readable error messages.

```scala
def withAgePlusOne(
  ageColName: String,
  resultColName: String
)(df: DataFrame): DataFrame = {
  validatePresenceOfColumns(df, Seq(ageColName, resultColName))
  df.withColumn(resultColName, col(ageColName) + 1)
}
```

## What type of DataFrame transformation should be used

Schema dependent transformations should be used for functions that rely on a large number of columns or functions that are only expected to be run on a certain schema (e.g. a data lake with a schema that doesn't change).

Schema independent transformations should be run for functions that will be run on DataFrames with different schemas

## None / null

`null` should be used in DataFrames for values that are [unknown, missing, or irrelevant](https://medium.com/@mrpowers/dealing-with-null-in-spark-cfdbb12f231e#.fk27ontik).

Spark core functions frequently return `null` and your code can also add `null` to DataFrames (by returning `None` or explicitly returning `null`).

Let's revisit the `with_full_name` function from earlier:

```scala
def with_full_name(df: DataFrame):
  df.withColumn(
    "full_name",
    concat_ws(" ", col("first_name"), col("last_name"))
  )
```

`with_full_name` returns `null` if either the `first_name` or `last_name` column is `null`.  Let's take a look at an example:

ADD EXAMPLE

The nullable property of a column should be set to `false` if the column should not take `null` values.

### Test suites

You should always test the `null` case to make sure that your code behaves as expected.

ADD DISUSSION

## Wheel Files

TODO: Add guidance

## Supporting multiple Spark versions

Libraries should generally support multiple Spark versions (multiple Python versions for that matter too).

It's OK for applications to only support a single Spark / Python version.

TODO: Add guidance on propertly supporting multiple Spark / Delta Lake versions.

## Documentation

TODO: Look how PySpark is documented and follow their examples exactly

## <a name='testing'>Testing</a>

Use the [chispa](https://github.com/MrPowers/chispa) library to unit test your Spark code.

TODO: Add more descriptions

## <a name='open-source'>Open Source</a>

You should write generic open source code whenever possible.  Open source code is easily reusable (especially when it's uploaded to PyPi) and forces you to abstract reusable chunks of open code from business logic.

TODO: Add quinn and mack examples

## <a name='best-practices'>Best Practices</a>

* Limit project dependencies and inspect transitive dependencies closely.  Don't depend on projects with lots of other dependencies.
* Write code that's easy to copy and paste in notebooks
* Organize code into column functions and custom transformations whenever possible
* Write code in version controlled projects, so you can take advantage of text editor features (and not pay for an expensive cluster when developing)
* Only surface a minimal public interface in your libraries
