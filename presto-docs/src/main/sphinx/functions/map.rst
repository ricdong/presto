===========================
Map Functions and Operators
===========================

Subscript Operator: []
----------------------

The ``[]`` operator is used to retrieve the value corresponding to a given key from a map::

    SELECT name_to_age_map['Bob'] AS bob_age

Map Functions
-------------

.. function:: map(array<K>, array<V>) -> map<K,V>

    Returns a map created using the given key/value arrays. ::

        SELECT MAP(ARRAY[1,3], ARRAY[2,4]); => {1 -> 2, 3 -> 4}

.. function:: cardinality(x) -> bigint
    :noindex:

    Returns the cardinality (size) of the map ``x``.

.. function:: map_keys(x<K,V>) -> array<K>

    Returns all the keys in the map ``x``.

.. function:: map_values(x<K,V>) -> array<V>

    Returns all the values in the map ``x``.

See also :func:`map_agg` for creating a map as an aggregation.
