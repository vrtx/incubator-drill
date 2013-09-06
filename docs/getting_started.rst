===============
Getting Started
===============
|

- `Download`_
- `Running Drill Locally`_
- `Using the Shell`_
    - `Querying a Local Data Source`_
    - `Querying an HDFS Data Source`_
- `Accessing Drill`_
    - ODBC/JDBC
    - Java
    - Other Languages
- `Next Steps`_
    - `Deploying Drill`_

|

.. _Installation:

Download
--------

Download `Apache Drill`_ and extract the contents:

::

    $ tar -zxf apache-drill-1.0.0-m1-binary-release.tar.gz
    $ cd apache-drill-1.0.0-m1/
    $ ls -l bin/
      drillbit.sh         - Drillbit daemon script
      sqlline             - Start a SQL shell with a local drillbit server
      submit_plan         - Submit a SQL statement, logical or physical plan to a running drillbit

|

.. _Running Drill Locally:

Running Drill Locally
---------------------
|

To start a single drillbit, simply execute the following command:

::

    $ ./bin/drillbit.sh start

|

The damon script can be configured using the following environment variables:

::

    DRILL_CONF_DIR      Alternate drill conf dir. Default is ${DRILL_HOME}/conf.
    DRILL_LOG_DIR       Where log files are stored.  /var/log/drill by default.
    DRILL_PID_DIR       The pid files are stored. /tmp by default.
    DRILL_IDENT_STRING  A string representing this instance of drillbit. $USER by default
    DRILL_NICENESS      The scheduling priority for daemons. Defaults to 0.
    DRILL_STOP_TIMEOUT  Time, in seconds, after which we kill -9 the server if it has not
                        stopped.  Default 1200 seconds.

|

If you need to override any of these settings (e.g. ``DRILL_LOG_DIR``), use the following command:

::

    $ env DRILL_LOG_DIR=. ./bin/drillbit.sh  start

|

At this point, Drill should be running. If you encounter any problems, errors can be found in ``drillbit.log``
and ``drillbit.out``.  Please feel free to ask questions on our `user mailing list`_, or file `a new issue`_ for
any bug reports or feature requests.  Please attach any relevant portions of the log file.

.. _Using the Shell:

Using the Shell
---------------

Apache Drill includes a shell based on SQLLine, which can be accessed by running ``./bin/sqllline`` and specifying
a JDBC driver.  Note that SQLLine automatically starts a local DrillBit, so there is no need to start a daemon process.

A JDBC driver must be supplied to SQLLine; e.g. ``-u jdbc:drill:schem=json-local``.  **TODO:** The current list of available drivers are available here.

 **NOTE:** For now, the top-level field in Drill is generally a MAP (e.g. JSON Object, column family, etc.).  Thus, field names will generally need to be wrapped in ``_MAP[' ']``.

|
|

.. _Querying a Local Data Source:

==================
Local Data Sources
==================
|

JSON
----

A simple JSON dataset such as:

::

    {
        name: 'Adam',
        age: '41'
    }
    {
        name: 'Jane',
        age: '42'
    }

Can be queried with a SQL statement such as:

::

    jdbc:drill:schema=json> SELECT _MAP['name'], _MAP['age'] FROM "sample-data/test.json";


Parquet
-------

Several sample datasets are included in the ``sample-data`` directory in parquet format.  SQLLine can be used to query
these files directly; for example:

::

    $ ./sqlline -u jdbc:drill:schema=parquet-local

    jdbc:drill:schema=parquet-local> SELECT _MAP['N_NAME']
                                        FROM "sample-data/nation.parquet"
                                        ORDER BY _MAP['N_NAME'] DESC;

.. _Querying an HDFS Data Source:

|
|

=================
HDFS Data Sources
=================
|
|
|
|

.. _Accessing Drill:

===============
Accessing Drill
===============
|
|

JDBC
----
|
|

ODBC
----
|
|

.. _Next Steps:

==========
Next Steps
==========
|
|

.. _Deploying Drill:

Deploying Drill
---------------
| Link to deployment strategies...
|
|


..
.. External Links
..
.. _Apache Drill: http://people.apache.org/~jacques/apache-drill-1.0.0-m1.rc3/apache-drill-1.0.0-m1-binary-release.tar.gz
.. _a new issue: https://issues.apache.org/jira/browse/DRILL
.. _user mailing list: http://mail-archives.apache.org/mod_mbox/incubator-drill-user/
