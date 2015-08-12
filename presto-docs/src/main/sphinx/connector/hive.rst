==============
Hive Connector
==============

The Hive connector allows querying data stored in a Hive
data warehouse. Hive is a combination of three components:

* Data files in varying formats that are typically stored in the
  Hadoop Distributed File System (HDFS) or in Amazon S3.
* Metadata about how the data files are mapped to schemas and tables.
  This metadata is stored in a database such as MySQL and is accessed
  via the Hive metastore service.
* A query language called HiveQL. This query language is executed
  on a distributed computing framework such as MapReduce or Tez.

Presto only uses the first two components: the data and the metadata.
It does not use HiveQL or any part of Hive's execution environment.

Configuration
-------------

Presto includes Hive connectors for multiple versions of Hadoop:

* ``hive-hadoop1``: Apache Hadoop 1.x
* ``hive-hadoop2``: Apache Hadoop 2.x
* ``hive-cdh4``: Cloudera CDH 4
* ``hive-cdh5``: Cloudera CDH 5

Create ``etc/catalog/hive.properties`` with the following contents
to mount the ``hive-cdh4`` connector as the ``hive`` catalog,
replacing ``hive-cdh4`` with the proper connector for your version
of Hadoop and ``example.net:9083`` with the correct host and port
for your Hive metastore Thrift service:

.. code-block:: none

    connector.name=hive-cdh4
    hive.metastore.uri=thrift://example.net:9083

Multiple Hive Clusters
^^^^^^^^^^^^^^^^^^^^^^

You can have as many catalogs as you need, so if you have additional
Hive clusters, simply add another properties file to ``etc/catalog``
with a different name (making sure it ends in ``.properties``). For
example, if you name the property file ``sales.properties``, Presto
will create a catalog named ``sales`` using the configured connector.
If you are connecting to more than one Hive metastore, you can create
any number of properties files configuring multiple instances of
the Hive connector.

HDFS Configuration
^^^^^^^^^^^^^^^^^^

Presto configures the HDFS client automatically for most setups and
does not require any configuration files. In some rare cases, such
as when using federated HDFS, it may be necessary to specify additional
HDFS client options in order to access your HDFS cluster. To do so, add
the ``hive.config.resources`` property to reference your HDFS config files:

.. code-block:: none

    hive.config.resources=/etc/hadoop/conf/core-site.xml,/etc/hadoop/conf/hdfs-site.xml

Only specify additional configuration files if absolutely necessary.
We also recommend reducing the configuration files to have the minimum
set of required properties, as additional properties may cause problems.

Configuration Properties
------------------------

================================================== ============================================================ ==========
Property Name                                      Description                                                  Example
================================================== ============================================================ ==========
``hive.metastore.uri``                             The URI of the Hive Metastore to connect to using            ``thrift://192.0.2.3:9083``
                                                   the Thrift protocol. This property is required.

``hive.config.resources``                          An optional comma-separated list of HDFS                     ``/etc/hdfs-site.xml``
                                                   configuration files. These files must exist on the
                                                   machines running Presto. Only specify this if
                                                   absolutely necessary to access HDFS.

``hive.storage-format``                            The default file format used when creating new tables        ``RCBINARY``

``hive.force-local-scheduling``                    Force splits to be scheduled on the same node as the Hadoop  ``true``
                                                   DataNode process serving the split data.  This is useful for
                                                   installations where Presto is collocated with every
                                                   DataNode.
================================================== ============================================================ ==========

Querying Hive Tables
--------------------

The following table is an example Hive table from the `Hive Tutorial`_.
It can be created in Hive (not in Presto) using the following
Hive ``CREATE TABLE`` command:

.. _Hive Tutorial: https://cwiki.apache.org/confluence/display/Hive/Tutorial#Tutorial-UsageandExamples

.. code-block:: none

    hive> CREATE TABLE page_view (
        >   viewTime INT,
        >   userid BIGINT,
        >   page_url STRING,
        >   referrer_url STRING,
        >   ip STRING COMMENT 'IP Address of the User')
        > COMMENT 'This is the page view table'
        > PARTITIONED BY (dt STRING, country STRING)
        > STORED AS SEQUENCEFILE;
    OK
    Time taken: 3.644 seconds

Assuming that this table was created in the ``web`` schema in
Hive, this table can be described in Presto::

    DESCRIBE hive.web.page_view;

.. code-block:: none

        Column    |  Type   | Null | Partition Key |        Comment
    --------------+---------+------+---------------+------------------------
     viewtime     | bigint  | true | false         |
     userid       | bigint  | true | false         |
     page_url     | varchar | true | false         |
     referrer_url | varchar | true | false         |
     ip           | varchar | true | false         | IP Address of the User
     dt           | varchar | true | true          |
     country      | varchar | true | true          |
    (7 rows)

This table can then be queried in Presto::

    SELECT * FROM hive.web.page_view;
