# PySpark Style Guide

PySpark provides you with access to Python language bindings to the Apache Spark big data engine.

This document outlines the best practices you should follow when writing PySpark code.

Automatic Python code formatting tools already exist so this document focuses specifically on PySpark best practices and how to structure PySpark code, not on general Python code formatting.

## Imports

Import the PySpark SQL functions into a variable named `F` to avoid polluting the global namespace:

```python
from pyspark.sql import functions as F
```

Similarly, import the types into a variable `T`:


```python
from pyspark.sql import types as T
```

You can structure libraries such that the public interface is exposed in a single namespace, so its easy to identify the source of subsequent function invocations.  Here's an example with the [quinn library](https://github.com/MrPowers/quinn):

```python
import quinn

quinn.validate_absence_of_columns(df, ["age", "cool"])
```

This import style makes it easy to identify where the `validate_absence_of_columns` function was defined.

You can also use this import style:

```python
from quinn import validate_absence_of_columns
```

Don't follow this import style that makes it hard to determine where `validate_absence_of_columns` comes from:

```python
from quinn import *
```

## Column functions

Here's an example of a column function that returns `child` when the age is less than 13, `teenager` when the age is between 13 and 19, and `adult` when the age is above 19.

```python
def life_stage(col):
    return (
        F.when(col < 13, "child")
        .when(col.between(13, 19), "teenager")
        .when(col > 19, "adult")
    )
```

The `life_stage()` function will return `null` when `col` is `null`.  All built-in Spark functions gracefully handle the `null` case, so we don't need to write explicit `null` logic in the `life_stage()` function.

Column functions can also be optimized by the Spark compiler, so this is a good way to write code.

## Schema Dependent Custom DataFrame Transformations

Custom DataFrame transformations are functions that take a DataFrame as an argument and return a DataFrame.  Custom DataFrame transformations are easy to test and reuse, so they're a good way to structure Spark code.

Let's wrap the `life_stage` column function that we previously defined in a schema dependent custom transformation.

```python
def with_life_stage(df):
    return df.withColumn("life_stage", life_stage(F.col("age")))
```

You can invoke this schema dependent custom DataFrame transformation with the `transform` method:

```python
df.transform(with_life_stage)
```

`with_life_stage` is an example of a schema dependent custom DataFrame transformation because it must be run on DataFrame that contains an `age` column.  Schema dependent custom DataFrame transformations make assumptions about the schema of the DataFrames they're run on.

## Schema Independent Custom DataFrame Transformations

Let's refactor the `with_life_stage` function so that it takes the column name as a parameter and does not depend on the underlying DataFrame schema (this syntax works as of PySpark 3.3.0).

```python
def with_life_stage2(df, col_name):
    return df.withColumn("life_stage", life_stage(F.col(col_name)))
```

There are two ways to invoke this schema independent custom DataFrame transformation:

```python
# invoke with a positional argument
df.transform(with_life_stage2, "age")

# invoke with a keyword argument
df.transform(with_life_stage2, col_name="age")
```

Read [this blog post](https://mungingdata.com/pyspark/chaining-dataframe-transformations/) for a full discussion on building custom transformation functions pre-PySpark 3.3.0.

## What type of DataFrame transformation should be used

Schema dependent transformations should be used for functions that rely on a large number of columns or functions that are only expected to be run on a certain schema (e.g. a data table with a schema that doesn't change).

Schema independent transformations should be run for functions that will be run on DataFrames with different schemas.

## Best practices for `None` and `null`

`null` should be used in DataFrames for values that are [unknown, missing, or irrelevant](https://medium.com/@mrpowers/dealing-with-null-in-spark-cfdbb12f231e#.fk27ontik).

Spark core functions frequently return `null` and your code can also add `null` to DataFrames (by returning `None` or relying on Spark functions that return `null`).

Let's take a look at a `with_full_name` custom DataFrame transformation:

```python
def with_full_name(df):
    return df.withColumn(
        "full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
    )
```

`with_full_name` returns `null` if either the `first_name` or `last_name` is `null`.  Let's take a look at an example:

Let's create a DataFrame to demonstrate the functionality of `with_full_name`:

```python
df = spark.createDataFrame(
    [("Marilyn", "Monroe"), ("Abraham", None), (None, None)]
).toDF("first_name", "last_name")
```

Here are the DataFrame contents:

```
+----------+---------+
|first_name|last_name|
+----------+---------+
|   Marilyn|   Monroe|
|   Abraham|     null|
|      null|     null|
+----------+---------+
```

Here's how to invoke the custom DataFrame transformation using the `transform` method:

```python
df.transform(with_full_name).show()
```

```
+----------+---------+--------------+
|first_name|last_name|     full_name|
+----------+---------+--------------+
|   Marilyn|   Monroe|Marilyn Monroe|
|   Abraham|     null|       Abraham|
|      null|     null|              |
+----------+---------+--------------+
```

The `nullable` property of a column should be set to `false` if the column should not take `null` values.  Look at the `nullable` properties in the resulting DataFrame.

```
df.transform(with_full_name).printSchema()

root
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- full_name: string (nullable = false)
```

`first_name` and `last_name` have `nullable` set to `true` because they can take `null` values.  `full_name` has `nullable` set to `false` because every value must be a non-null string.

See the section on User Defined Functions for more information about properly handling the `None` / `null` values for UDFs.

## Testing column functions

You can use the [chispa](https://github.com/MrPowers/chispa) library to unit test your PySpark column functions and custom DataFrame transformations.

Let's look at how to unit test the `life_stage` column function:

```python
def life_stage(col):
    return (
        F.when(col < 13, "child")
        .when(col.between(13, 19), "teenager")
        .when(col > 19, "adult")
    )
```

Create a test DataFrame with the expected return value of the function for each row:

```python
df = spark.createDataFrame(
    [
        ("karen", 56, "adult"),
        ("jodie", 16, "teenager"),
        ("jason", 3, "child"),
        (None, None, None),
    ]
).toDF("first_name", "age", "expected")
```

Now invoke the function and create the result DataFrame:

```python
res = df.withColumn("actual", life_stage(F.col("age")))
```

Assert that the actual return value equals the expected value:

```python
import chispa

chispa.assert_column_equality(res, "expected", "actual")
```

You should always test the `None` / `null` case to make sure that your code behaves as expected.

## Testing custom DataFrame transformations

Suppose you have the following custom transformation: 

```python
def with_full_name(df):
    return df.withColumn(
        "full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
    )
```

Create a minimalistic input DataFrame with some test data:

```python
input_df = spark.createDataFrame(
    [("Marilyn", "Monroe"), ("Abraham", None), (None, None)]
).toDF("first_name", "last_name")
```

Create a DataFrame with the expected data:

```python
expected_df = spark.createDataFrame(
    [
        ("Marilyn", "Monroe", "Marilyn Monroe"),
        ("Abraham", None, "Abraham"),
        (None, None, ""),
    ]
).toDF("first_name", "last_name", "full_name")
```

Make sure the expected DataFrame equals the input DataFrame with the custom DataFrame transformation applied:

```python
chispa.assert_df_equality(
    expected_df, input_df.transform(with_full_name), ignore_nullable=True
)
```

## Automatic code formatting

TODO: Determine if Black or Ruff should be recommended.

You should use [Black](https://github.com/psf/black) to automatically format your code in a PEP 8 compliant manner.

You should use automatic code formatting for both your projects and your notebooks.

## Typing

TODO: Add guidance

## Variable naming conventions

Variables should use `snake_case`, as required by Black.

Variables that point to DataFrames and RDDs should be suffixed to make your code readable:

* Variables pointing to DataFrames should be suffixed with `df`:

```python
people_df.createOrReplaceTempView("people")
```

* Variables pointing to RDDs should be suffixed with `rdd`:

```python
people_rdd = spark.sparkContext.textFile("examples/src/main/resources/people.txt")
```

Use the variable `col` for `Column` arguments.

```python
def min(col)
```

Use `col1` and `col2` for functions that take two `Column` arguments.

```python
def corr(col1, col2)
```

Use `cols` for functions that take an arbitrary number of `Column` arguments.

```python
def array(cols)
```

Use `col_name` for functions that take a `String` argument that refers to the name of a `Column`.

```python
def sqrt(col_name)
```

Use `col_name1` and `col_name2` for methods that take multiple column name arguments.

Collections should use plural variable names.

```python
animals = ["dog", "cat", "goose"]

# DON'T DO THIS
animal_list = ["dog", "cat", "goose"]
```

Singular nouns should be used for single objects.

```python
my_car_color = "red"
```

## Columns

Columns have name, type, nullable, and metadata properties.

Columns that contain boolean values should use predicate names like `is_nice_person` or `has_red_hair`.  Use `snake_case` for column names, so it's easier to write SQL code.

You can write `(col("is_summer") and col("is_europe"))` instead of `(col("is_summer") === true and col("is_europe") === true)`.  The predicate column names make the concise syntax readable.

Columns should be typed properly.  Don't overuse `StringType` columns.

Columns should only be nullable if `null` values are allowed.  Functions invoked on nullable columns should always handle `null` values gracefully.

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

## User defined functions

You can write User Defined Functions (UDFs) when you need to write code that leverages Python programming features / Python libraries that aren't accessible in Spark.

Here's an example of a UDF that appends "is fun" to a string:

```python
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf


@udf(returnType=StringType())
def bad_funify(s):
    return s + " is fun!"

```

The `bad_funify` function is poorly structured because it errors out when run on a column with `null` values.

Here's how to refactor the UDF to handle `null` input without erroring out:

```python

@udf(returnType=StringType())
def good_funify(s):
    return None if s == None else s + " is fun!"

```

In this case, a UDF isn't even necessary.  You can just define a regular column function to get the same functionality:

```python

def best_funify(col):
    return F.concat(col, F.lit(" is fun!"))
    
```

UDFs [are a black box](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-udfs-blackbox.html) from the Spark compiler's perspective and should be avoided whenever possible.

## Function naming conventions

* `with` precedes transformations that add columns:

  - `with_cool_cat()` adds the column `cool_cat` to a DataFrame

  - `with_is_nice_person()` adds the column `is_nice_person` to a DataFrame.

* `filter` precedes transformations that remove rows:

  - `filter_negative_growth_rate()` removes the data rows where the `growth_rate` column is negative

  - `filter_invalid_zip_codes()` removes the data with a malformed `zip_code`

* `enrich` precedes transformations that clobber columns.  Clobbing columns should be avoided when possible, so `enrich` transformations should only be used in rare circumstances.

* `explode` precedes transformations that add rows to a DataFrame by "exploding" a row into multiple rows.

## Virtual environment management

### uv

TODO

### venv

TODO

### Poetry

TODO

### Conda

TODO

## Wheel Files

TODO: Add guidance

## Supporting multiple Spark versions

Libraries should generally support multiple Spark versions (multiple Python versions for that matter too).  For example, the chispa library should work with many PySpark versions and chispa should intentionally avoid depending on new PySpark features that would compromise `pip install chispa` from working properly on older PySpark versions.

It's OK for applications to only support a single Spark/Python version.

TODO: Add guidance on propertly supporting multiple Spark/Delta Lake versions.

## Documentation

TODO: Look how PySpark is documented and follow their examples

## Open Source Library development

You should write generic open source code whenever possible.  Open source code is easily reusable (especially when it's uploaded to PyPi) and forces you to abstract reusable chunks of open code from business logic.

TODO: Add quinn and mack examples

* Support many PySpark versions
* Expose the public interface with a clean import interface
* Only surface a minimal public interface in your libraries

## Best Practices

* Limit project dependencies and inspect transitive dependencies closely.  Try to avoid depending on projects with lots of transitive dependencies.
* Write code that's easy to copy and paste in notebooks
* Organize code into column functions and custom transformations whenever possible
* Write code in version controlled projects, so you can take advantage of text editor features (and not pay for an expensive cluster when developing)

## Avoid overreliance on notebooks

TODO: Discuss why version control is important

## Continuous integration

TODO

## Continuous deployment

TODO

## Spark Connect compliance

TODO
