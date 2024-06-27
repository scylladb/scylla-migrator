=============================================
Migrate from Cassandra or from a Parquet File
=============================================

This page explains how to fill the ``source`` and ``target`` properties of the `configuration file </configuration>`_ to migrate data:

- from Cassandra, ScyllaDB, or from a `Parquet <https://parquet.apache.org/>`_ file,
- to Cassandra or ScyllaDB.

In file ``config.yaml``, make sure to keep only one ``source`` property and one ``target`` property, and configure them as explained in the following subsections according to your case.

----------------------
Configuring the Source
----------------------

The data `source` can be a Cassandra or ScyllaDB database, or a Parquet file.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Reading from Cassandra or ScyllaDB
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In both cases, when reading from Cassandra or ScyllaDB, the type of source should be ``cassandra`` in the configuration file. Here is a minimal ``source`` configuration:

.. code-block:: yaml

  source:
    type: cassandra
    # host name of one of the nodes of your database cluster
    host: <cassandra-server-01>
    # TCP port to use for CQL
    port: 9042
    # Keyspace in which the table is located
    keyspace: <keyspace>
    # Name of the table to read
    table: <table>
    # Consistency Level for the source connection.
    # Options are: LOCAL_ONE, ONE, LOCAL_QUORUM, QUORUM.
    # We recommend using LOCAL_QUORUM. If using ONE or LOCAL_ONE, ensure the source system is fully repaired.
    consistencyLevel: LOCAL_QUORUM
    # Preserve TTLs and WRITETIMEs of cells in the source database. Note that this
    # option is *incompatible* when copying tables with collections (lists, maps, sets).
    preserveTimestamps: true
    # Number of splits to use - this should be at minimum the amount of cores
    # available in the Spark cluster, and optimally more; higher splits will lead
    # to more fine-grained resumes. Aim for 8 * (Spark cores).
    splitCount: 256
    # Number of connections to use to Cassandra when copying
    connections: 8
    # Number of rows to fetch in each read
    fetchSize: 1000

Where the values ``<cassandra-server-01>``, ``<keyspace>``, and ``<table>`` should be replaced with your specific values.

Additionally, you can also set the following optional properties:

.. code-block:: yaml

  source:
    # ... same as above

    # Datacenter to use
    localDC: <datacenter>

    # Connection credentials
    credentials:
      username: <username>
      password: <pass>

    # SSL options as per https://github.com/scylladb/spark-cassandra-connector/blob/master/doc/reference.md#cassandra-ssl-connection-options
    sslOptions:
      clientAuthEnabled: false
      enabled: false
      # all below are optional! (generally just trustStorePassword and trustStorePath is needed)
      trustStorePassword: <pass>
      trustStorePath: <path>
      trustStoreType: JKS
      keyStorePassword: <pass>
      keyStorePath: <path>
      keyStoreType: JKS
      enabledAlgorithms:
       - TLS_RSA_WITH_AES_128_CBC_SHA
       - TLS_RSA_WITH_AES_256_CBC_SHA
      protocol: TLS

    # Condition to filter data that will be migrated
    where: race_start_date = '2015-05-27' AND race_end_date = '2015-05-27'

Where ``<datacenter>``, ``<username>``, ``<pass>``, ``<path>``, and the content of the ``where`` properties should be replaced with your specific values.

^^^^^^^^^^^^^^^^^^^^^^^^^^^
Reading from a Parquet File
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Migrator can read data from a Parquet file located on the filesystem of the Spark master node, or on an S3 bucket. In both cases, set the source type to ``parquet``. Here is a complete ``source`` configuration to read from the filesystem:

.. code-block:: yaml

  source:
    type: parquet
    path: /<my-directory/my-file.parquet>

Where ``<my-directory/my-file.parquet>`` should be replaced with your actual file path.

Here is a minimal ``source`` configuration to read the Parquet file from an S3 bucket:

.. code-block:: yaml

  source:
    type: parquet
    path: s3a://<my-bucket/my-key.parquet>

Where ``<my-bucket/my-key.parquet>`` should be replaced with your actual S3 bucket and key.

In case the object is not public in the S3 bucket, you can provide the AWS credentials to use as follows:

.. code-block:: yaml

  source:
    type: parquet
    path: s3a://my-bucket/my-key.parquet
    credentials:
      accessKey: <access-key>
      secretKey: <secret-key>

Where ``<access-key>`` and ``<my-secret-key>`` should be replaced with your actual AWS access key and secret key.

The Migrator also supports advanced AWS authentication options such as using `AssumeRole <https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_cross-account-with-roles.html>`_. Please read the `configuration reference </configuration#aws-authentication>` for more details.

---------------------------
Configuring the Destination
---------------------------



