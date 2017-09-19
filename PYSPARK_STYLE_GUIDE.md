# Pyspark Style Guide

## <a name='imports'>Imports</a>

Import the Pyspark SQL functions into a variable named `F` to avoid polluting the global namespace.

```python
from pyspark.sql import functions as F
```

Also import user defined functions into named variables.  For example, import the used defined functions defined in the [quinn library](https://github.com/MrPowers/quinn) as follows:

```python
import quinn.functions as QF
```
