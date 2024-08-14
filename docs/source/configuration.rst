=======================
Configuration Reference
=======================

This page documents the schema of the YAML configuration file used by the Migrator and the Validator.

The configuration file is a YAML object whose fields are enumerated, preceded by a comment describing their role. All the fields are mandatory except if their documentation starts with “Optional”. All the values of the form ``<xxx>`` are placeholders that should be replaced with your specific settings.

The YAML format is whitespace sensitive, make sure to use the proper number of spaces to keep on the same level of indentation all the properties that belong to the same object. If the configuration file is not correctly formatted, the Migrator will fail at startup with a message like “DecodingFailure at ...” or “ParsingFailure ...” describing the problem. For instance, the following line in the logs means that the mandatory property ``target`` is missing from the configuration file:

.. code-block:: text

  Exception in thread "main" DecodingFailure at .target: Missing required field

--------
Overview
--------

The configuration file requires the following top-level properties (ie, with no leading space before the property names), which are documented further below:

.. code-block:: yaml

  # Source configuration
  source:
    # ...
  # Target configuration
  target:
    # ...
  # Optional - Columns to rename
  renames:
    # ...
  # Savepoints configuration
  savepoints:
    # ...
  # Validator configuration. Required only if the app is executed in validation mode.
  validation:
    # ...
  # Optional- Used internally
  skipTokenRanges: []

These top-level properties are documented in the following sections (except ``skipTokenRanges``, which is used internally).

------
Source
------

The ``source`` property describes the type of data to read from. It must be an object with a field ``type`` defining the type of source, and other fields depending on the type of source.

Valid values for the source ``type`` are:

- ``cassandra`` for a CQL-compatible source (Apache Cassandra or ScyllaDB).
- ``parquet`` for a dataset stored using the Parquet format.
- ``dynamodb`` for a DynamoDB-compatible source (AWS DynamoDB or ScyllaDB Alternator).
- ``dynamodb-s3-export`` for a DynamoDB table exported to S3.

The following subsections detail the schema of each source type.

^^^^^^^^^^^^^^^^^^^^^^^
Apache Cassandra Source
^^^^^^^^^^^^^^^^^^^^^^^

A source of type ``cassandra`` can be used together with a target of type ``cassandra`` only.

.. code-block:: yaml

  source:
    type: cassandra
    # Host name of one of the nodes of your database cluster
    host: <cassandra-server-01>
    # TCP port to use for CQL
    port: 9042
    # Optional - Connection credentials
    credentials:
      username: <username>
      password: <pass>
    # Optional - Datacenter to use
    localDC: <datacenter>
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
    # Number of connections to use to Apache Cassandra when copying
    connections: 8
    # Number of rows to fetch in each read
    fetchSize: 1000
    # Optional - SSL options as per https://github.com/scylladb/spark-cassandra-connector/blob/master/doc/reference.md#cassandra-ssl-connection-options
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
    # Optional - Condition to filter data that will be migrated
    where: race_start_date = '2015-05-27' AND race_end_date = '2015-05-27'

^^^^^^^^^^^^^^
Parquet Source
^^^^^^^^^^^^^^

A source of type ``parquet`` can be used together with a target of type ``cassandra`` only.

.. code-block:: yaml

  source:
    type: parquet
    # Path of the Parquet file.
    # It can be a file located on the Spark master node filesystem (e.g. '/some-directory/some-file.parquet'),
    # or a file stored on S3 (e.g. 's3a://some-bucket/some-file.parquet')
    path: <path>
    # Optional - in case of a file stored on S3, the AWS credentials to use
    credentials:
      # ... see the “AWS Authentication” section below

^^^^^^^^^^^^^^^
DynamoDB Source
^^^^^^^^^^^^^^^

A source of type ``dynamodb`` can be used together with a target of type ``dynamodb`` only.

.. code-block:: yaml

  source:
    type: dynamodb
    # Name of the table to write. If it does not exist, it will be created on the fly.
    table: <table>
    # Connect to a custom endpoint. Mandatory if writing to ScyllaDB Alternator.
    endpoint:
      # If writing to ScyllaDB Alternator, prefix the hostname with 'http://'.
      host: <host>
      port: <port>
    # Optional - AWS availability region.
    region: <region>
    # Optional - Authentication credentials. See the section “AWS Authentication” for more details.
    credentials:
      accessKey: <access-key>
      secretKey: <secret-key>
    # Optional - Split factor for reading. The default is to split the source data into chunks
    # of 128 MB that can be processed in parallel by the Spark executors.
    scanSegments: 1
    # Optional - Throttling settings, set based on your database read capacity units (or wanted capacity)
    readThroughput: 1
    # Optional - Can be between 0.1 and 1.5, inclusively.
    # 0.5 represents the default read rate, meaning that the job will attempt to consume half of the read capacity of the table.
    # If you increase the value above 0.5, spark will increase the request rate; decreasing the value below 0.5 decreases the read request rate.
    # (The actual read rate will vary, depending on factors such as whether there is a uniform key distribution in the DynamoDB table.)
    throughputReadPercent: 1.0
    # Optional - At most how many tasks per Spark executor? The default is to use the same as 'scanSegments'.
    maxMapTasks: 1

The properties ``scanSegments`` and ``maxMapTasks`` can have significant impact on the migration throughput. By default, the migrator splits the data into segments of 128 MB each.

Use ``maxMapTasks`` to cap the parallelism level used by the Spark executor when processing each segment.

^^^^^^^^^^^^^^^^^^^^^^^^^
DynamoDB S3 Export Source
^^^^^^^^^^^^^^^^^^^^^^^^^

A source of type ``dynamodb-s3-export`` can be used together with a target of type ``dynamodb`` only.

.. code-block:: yaml

  source:
    type: dynamodb-s3-export
    # Name of the S3 bucket where the DynamoDB table has been exported
    bucket: <bucket-name>
    # Key of the `manifest-summary.json` object in the bucket
    manifestKey: <manifest-summary-key>
    # Optional - Connect to a custom endpoint instead of the standard AWS S3 endpoint
    endpoint:
      # Specify the hostname without a protocol
      host: <host>
      port: <port>
    # Optional - AWS availability region
    region: <region>
    # Optional - Connection credentials. See the section “AWS Authentication” below for more details.
    credentials:
      accessKey: <access-key>
      secretKey: <secret-key>
    # Key schema and attribute definitions, see https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TableCreationParameters.html
    tableDescription:
      # See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeDefinition.html
      attributeDefinitions:
        - name: <attribute-name>
          type: <attribute-type>
        # ... other attributes
      # See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_KeySchemaElement.html
      keySchema:
        - name: <key-name>
          type: <key-type>
        # ... other key schema definitions
    # Optional - Whether to use “path-style access” in S3 (see https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html). Default is false.
    usePathStyleAccess: true

------
Target
------

The ``target`` property describes the type of data to write. It must be an object with a field ``type`` defining the type of target, and other fields depending on the type of target.

Valid values for the target ``type`` are:

- ``cassandra`` for a CQL-compatible target (Apache Cassandra or ScyllaDB).
- ``dynamodb`` for a DynamoDB-compatible target (DynamoDB or ScyllaDB Alternator).

The following subsections detail the schema of each target type.

^^^^^^^^^^^^^^^^^^^^^^^
Apache Cassandra Target
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: yaml

  target:
    type: cassandra
    # Host name of one of the nodes of your target database cluster
    host: <scylla-server-01>
    # TCP port for CQL
    port: 9042
    # Keyspace to use
    keyspace: <keyspace>
    # Optional - Datacenter to use
    localDC: <datacenter>
    # Optional - Authentication credentials
    credentials:
      username: <username>
      password: <pass>
    # Name of the table to write. If it does not exist, it will be created on the fly.
    # It has to have the same schema as the source table. If needed, you can rename
    # columns along the way, look at the documentation page “Rename Columns”.
    table: <table>
    # Consistency Level for the target connection
    # Options are: LOCAL_ONE, ONE, LOCAL_QUORUM, QUORUM.
    consistencyLevel: LOCAL_QUORUM
    # Number of connections to use to ScyllaDB / Apache Cassandra when copying
    connections: 16
    # Spark pads decimals with zeros appropriate to their scale. This causes values
    # like '3.5' to be copied as '3.5000000000...' to the target. There's no good way
    # currently to preserve the original value, so this flag can strip trailing zeros
    # on decimal values before they are written.
    stripTrailingZerosForDecimals: false
    # Optional - If we do not persist timestamps (when preserveTimestamps is false in the source)
    # we can enforce in writer a single TTL or writetimestamp for ALL written records.
    # Such writetimestamp can be e.g. set to time BEFORE starting dual writes,
    # and this will make your migration safe from overwriting dual write
    # even for collections.
    # ALL rows written will get the same TTL or writetimestamp or both
    # (you can uncomment just one of them, or all or none)
    # TTL in seconds (sample 7776000 is 90 days)
    writeTTLInS: 7776000
    # Optional - writetime in microseconds (sample 1640998861000 is Saturday, January 1, 2022 2:01:01 AM GMT+01:00 )
    writeWritetimestampInuS: 1640998861000
    # Optional - SSL as per https://github.com/scylladb/spark-cassandra-connector/blob/master/doc/reference.md#cassandra-ssl-connection-options
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


^^^^^^^^^^^^^^^
DynamoDB Target
^^^^^^^^^^^^^^^

.. code-block:: yaml

  target:
    type: dynamodb
    # Name of the table to write. If it does not exist, it will be created on the fly.
    table: <table>
    # Optional - Throttling settings, set based on your database write capacity units (or wanted capacity).
    # By default, for provisioned tables we use the configured write capacity units, and for on-demand tables we use the value 40000.
    writeThroughput: 1
    # Optional - Can be between 0.1 and 1.5, inclusively.
    # 0.5 represents the default write rate, meaning that the job will attempt to consume half of the write capacity of the table.
    # If you increase the value above 0.5, spark will increase the request rate; decreasing the value below 0.5 decreases the write request rate.
    # (The actual write rate will vary, depending on factors such as whether there is a uniform key distribution in the DynamoDB table.)
    throughputWritePercent: 1.0
    # When transferring DynamoDB sources to DynamoDB targets (such as other DynamoDB tables or Alternator tables),
    # the migrator supports transferring live changes occurring on the source table after transferring an initial
    # snapshot.
    # Please see the documentation page “Stream Changes” for more details about this option.
    streamChanges: false
    # Optional - When streamChanges is true, skip the initial snapshot transfer and only stream changes.
    # This setting is ignored if streamChanges is false.
    skipInitialSnapshotTransfer: false

-------
Renames
-------

The optional ``renames`` property lists the item columns to rename along the migration.

.. code-block:: yaml

  renames:
    - from: <source-column-name>
      to: <target-column-name>
    # ... other columns to rename

----------
Savepoints
----------

When migrating data over CQL-compatible storages, the migrator is able to resume an interrupted migration. To achieve this, it stores so-called “savepoints” along the process to remember which token have already been migrated and should be skipped when the migration is restarted. This feature is not supported by DynamoDB-compatible storages.

.. code-block:: yaml

  savepoints:
    # Whe should savepoint configurations be stored? This is a path on the host running
    # the Spark driver - usually the Spark master.
    path: /app/savepoints
    # Interval in which savepoints will be created
    intervalSeconds: 300

----------
Validation
----------

The ``validation`` field and its properties are mandatory only when the application is executed in :doc:`validation mode </validate>`.

.. code-block:: yaml

  validation:
    # Should WRITETIMEs and TTLs be compared?
    compareTimestamps: true
    # What difference should we allow between TTLs?
    ttlToleranceMillis: 60000
    # What difference should we allow between WRITETIMEs?
    writetimeToleranceMillis: 1000
    # How many differences to fetch and print
    failuresToFetch: 100
    # What difference should we allow between floating point numbers?
    floatingPointTolerance: 0.001
    # What difference in ms should we allow between timestamps?
    timestampMsTolerance: 0

------------------
AWS Authentication
------------------

When reading from DynamoDB or S3, or when writing to DynamoDB, the communication with AWS can be configured with the properties ``credentials``, ``endpoint``, and ``region`` in the configuration:

.. code-block:: yaml

  credentials:
    accessKey: <access-key>
    secretKey: <secret-key>
  # Optional - AWS endpoint configuration
  endpoint:
    host: <host>
    port: <port>
  # Optional - AWS availability region, required if you use a custom endpoint
  region: <region>

Additionally, you can authenticate with `AssumeRole <https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_cross-account-with-roles.html>`_. In such a case, the ``accessKey`` and ``secretKey`` are the credentials of the user whose access to the resource (DynamoDB table or S3 bucket) has been granted via a “role”, and you need to add the property ``assumeRole`` as follows:

.. code-block:: yaml

  credentials:
    accessKey: <access-key>
    secretKey: <secret-key>
    assumeRole:
      arn: <role-arn>
      # Optional - Session name to use. If not set, we use 'scylla-migrator'.
      sessionName: <role-session-name>
  # Note that the region is mandatory when you use `assumeRole`
  region: <region>
