===============
Getting Started
===============

- `Installation`_
- `Running Drill Locally`_
- `Using the Shell`_
- `Accessing Drill`_
- `JDBC`_
- `Getting Help`_


.. _Installation:

Installation
------------

The simplest way to get started is to download the latest `Apache Drill`_ release and extract the contents:

::

    $ tar -zxf apache-drill-1.0.0-m1-binary-release.tar.gz
    $ cd apache-drill-1.0.0-m1/
    $ ls -l bin/
      drillbit.sh         - Drillbit daemon script
      sqlline             - Start a SQL shell with a local drillbit server
      submit_plan         - Submit a SQL statement, logical or physical plan to a running drillbit

No installation is neccesary to run Drill.  Details about package-based installation and startup
scripts are forthcoming.  Alternatively, Drill can always be `built directly from source`_.


.. _Running Drill Locally:

Running Drill Locally
---------------------

To start a single DrillBit daemon, simply execute the following command:

::

    $ ./bin/drillbit.sh start

The damon script can be configured using the following environment variables:

::

    DRILL_CONF_DIR      Alternate drill conf dir. Default is ${DRILL_HOME}/conf.
    DRILL_LOG_DIR       Where log files are stored.  /var/log/drill by default.
    DRILL_PID_DIR       The pid files are stored. /tmp by default.
    DRILL_IDENT_STRING  A string representing this instance of drillbit. $USER by default
    DRILL_NICENESS      The scheduling priority for daemons. Defaults to 0.
    DRILL_STOP_TIMEOUT  Time, in seconds, after which we kill -9 the server if it has not
                        stopped.  Default 1200 seconds.

If you need to override any of these settings (e.g. ``DRILL_LOG_DIR``), use the following command:

::

    $ env DRILL_LOG_DIR=. ./bin/drillbit.sh start

At this point, a local Drill server should be running.  If you encounter any problems, please see ``drillbit.log``
and ``drillbit.out``.


.. _Using the Shell:

Using the Shell
---------------

Apache Drill includes a shell based on SQLLine.  To start the shell, simply run ``./bin/sqllline``
and specify a JDBC driver (e.g. ``./bin/sqlline -u jdbc:drill:schema=parquet-local``).  [TODO:] List
all supported drivers.

    **NOTE:** The shell contains an embedded DrillBit server, so there is no need to start a
    daemon process as outlined above.

|

    **NOTE:** For now, the top-level field in Drill is a MAP (e.g. JSON Object, column
    family, etc.).  Thus, field names will generally need to be wrapped in ``_MAP[' ']``.

The following example illustrates a querying one of the sample datasets in parquet format:

::

    $ ./bin/sqlline -u jdbc:drill:schema=parquet-local

    jdbc:drill:schema=parquet-local> SELECT _MAP['N_NAME']
                                      FROM  "sample-data/nation.parquet"
                                      ORDER BY _MAP['N_NAME'] DESC;

Similarly, the Drill shell can be used to query a JSON file directly:

::

    $ cat users.json
    { name: 'Adam',  age: '41' }
    { name: 'Jane',  age: '42' }

    $ ./bin/sqlline -u jdbc:drill:schema=json

    jdbc:drill:schema=json> SELECT _MAP['name'], _MAP['age']
                             FROM  "users.json";


.. _Accessing Drill:

===============
Accessing Drill
===============

Drill can be queried using a number of query languages, primarily SQL.  Other languages (e.g.
DrillQL, MongoQL, etc.) are planned for the future.

For development and debugging purposes, it's also possible to submit a logical plan or physical plan directly to
Drill using the submit_plan utility.  Details can be found in the `QuerySubmitter class`_.

The most common way of accessing Drill is via JDBC.

JDBC
----
|
|


.. _Getting Help:

Getting Help
------------

Please address any questions to our `user mailing list`_, or file `a new issue`_ for bug reports
or feature requests.  Be sure to attach any relevant queries, data and portions of the log file.


..
.. External Links
..
.. _Apache Drill: http://people.apache.org/~jacques/apache-drill-1.0.0-m1.rc3/apache-drill-1.0.0-m1-binary-release.tar.gz
.. _a new issue: https://issues.apache.org/jira/browse/DRILL
.. _user mailing list: http://mail-archives.apache.org/mod_mbox/incubator-drill-user/
.. _built directly from source: https://cwiki.apache.org/confluence/display/DRILL/Sources+and+Setting+Up+Development+Environment
.. _wiki: https://cwiki.apache.org/confluence/display/DRILL/
.. _QuerySubmitter class: https://github.com/apache/incubator-drill/blob/master/exec/java-exec/src/main/java/org/apache/drill/exec/client/QuerySubmitter.java