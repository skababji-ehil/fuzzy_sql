Usage
=====
fuzz_tabular()
--------------
The core function for generating random queries is ``fuzz_tabular()``. Here is a description of the function:

.. autofunction:: fuzzy_sql.fuzzy_sql.fuzz_tabular

Here is an example how to generate 10 queries for a single  dataset:

.. code-block:: python
    
    queries=fuzzy_sql.fuzz_tabular(10,,"single_fltr","path/to/file/X_real.csv", "path/to/file/X_metadata.json")

Below is another example to generate 100 aggregate queries simultaneously applied to both real and synthetic input datatsets:

.. code-block:: python

    queries=fuzzy_sql.fuzz_tabular(100,"twin_agg","path/to/file/X_real.csv", "path/to/file/X_metadata.json", "path/to/file/X_syn.csv")



**Note**: Windows users may need to add 'r' before the path string they pass to the function. This will force treating windows backslashes as literal raw character. For instance, pass: ``r"C:\path\to\file\X_real.csv"``




