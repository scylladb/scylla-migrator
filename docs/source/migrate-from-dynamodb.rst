=====================
Migrate from DynamoDB
=====================

This page explains how to fill the ``source`` and ``target`` properties of the :doc:`configuration file </configuration>` to migrate data:

- from a DynamoDB table, a ScyllaDB Alternator table, or a `DynamoDB S3 export <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.HowItWorks.html>`_,
- to a DynamoDB table or a ScyllaDB Alternator table.

In file ``config.yaml``, make sure to keep only one ``source`` property and one ``target`` property, and configure them as explained in the following subsections according to your case.

----------------------
Configuring the Source
----------------------

The data ``source`` can be a DynamoDB or Alternator table, or a DynamoDB S3 export.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Reading from DynamoDB or Alternator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In both cases, when reading from DynamoDB or Alternator, the type of source should be ``dynamodb`` in the configuration file. Here is a minimal ``source`` configuration to read a DynamoDB table:

.. code-block:: yaml

  source:
    type: dynamodb
    table: <table>
    region: <region>

Where ``<table>`` is the name of the table to read, and ``<region>`` is the AWS region where the DynamoDB instance is located.

To read from the Alternator, you need to provide an ``endpoint`` instead of a ``region``:

.. code-block:: yaml

  source:
    type: dynamodb
    table: <table>
    endpoint:
      host: http://<host>
      port: <port>

Where ``<host>`` and ``<port>`` should be replaced with the host name and TCP port of your Alternator instance.

In practice, your source database (DynamoDB or Alternator) may require authentication. You can provide the AWS credentials with the ``credentials`` property:

.. code-block:: yaml

  source:
    type: dynamodb
    table: <table>
    region: <region>
    credentials:
      accessKey: <access-key>
      secretKey: <secret-key>

Where ``<access-key>`` and ``<secret-key>`` should be replaced with your actual AWS access key and secret key.

The Migrator also supports advanced AWS authentication options such as using `AssumeRole <https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_cross-account-with-roles.html>`_. Please read the :ref:`configuration reference <aws-authentication>` for more details.

Last, you can provide the following optional properties:

.. code-block:: yaml

  source:
    # ... same as above

    # Split factor for reading. The default is to split the source data into chunks
    # of 128 MB that can be processed in parallel by the Spark executors.
    scanSegments: 1
    # Throttling settings, set based on your database capacity (or wanted capacity)
    readThroughput: 1
    # Can be between 0.1 and 1.5, inclusively.
    # 0.5 represents the default read rate, meaning that the job will attempt to consume half of the read capacity of the table.
    # If you increase the value above 0.5, spark will increase the request rate; decreasing the value below 0.5 decreases the read request rate.
    # (The actual read rate will vary, depending on factors such as whether there is a uniform key distribution in the DynamoDB table.)
    throughputReadPercent: 1.0
    # At most how many tasks per Spark executor? The default is to use the same as 'scanSegments'.
    maxMapTasks: 1

^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Reading a DynamoDB S3 Export
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To read the content of a `DynamoDB table exported to S3 <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.HowItWorks.html>`_, use the ``source`` type ``dynamodb-s3-export``. Here is a minimal source configuration:

.. code-block:: yaml

  source:
    type: dynamodb-s3-export
    # Name of the S3 bucket where the DynamoDB table has been exported
    bucket: <bucket-name>
    # Key of the `manifest-summary.json` object in the bucket
    manifestKey: <manifest-summary-key>
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

Where ``<bucket-name>``, ``<manifest-summary-key>``, ``<attribute-name>``, ``<attribute-type>``, ``<key-name>``, and ``<key-type>`` should be replaced with your specific values.

Here is a concrete example assuming the export manifest summary URL is ``s3://my-dynamodb-exports/my-path/AWSDynamoDB/01234567890123-1234abcd/manifest-summary.json``, and the table only uses a single text attribute ``id`` as a partition key:

.. code-block:: yaml

  source:
    type: dynamodb-s3-export
    bucket: my-dynamodb-exports
    manifestKey: my-path/AWSDynamoDB/01234567890123-1234abcd/manifest-summary.json
    tableDescription:
      attributeDefinitions:
        - name: id
          type: S
      keySchema:
        - name: id
          type: HASH

Additionally, you can provide the following optional properties:

.. code-block:: yaml

  source:
    # ... same as above

    # Connect to a custom endpoint instead of the standard AWS S3 endpoint
    endpoint:
      # Specify the hostname without a protocol
      host: <host>
      port: <port>

    # AWS availability region
    region: <region>

    # Connection credentials:
    credentials:
      accessKey: <access-key>
      secretKey: <secret-key>

    # Whether to use “path-style access” in S3 (see https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html). Default is false.
    usePathStyleAccess: true

Where ``<host>``, ``<port>``, ``<region>``, ``<access-key>``, and ``<secret-key>`` should be replaced with your specific values.

The Migrator also supports advanced AWS authentication options such as using `AssumeRole <https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_cross-account-with-roles.html>`_. Please read the :ref:`configuration reference <aws-authentication>` for more details.

---------------------------
Configuring the Destination
---------------------------

The migration ``target`` can be DynamoDB or Alternator. In both cases, we use the configuration type ``dynamodb`` in the configuration. Here is a minimal ``target`` configuration to write to DynamoDB or Alternator:

.. code-block:: yaml

  target:
    type: dynamodb
    # Name of the table to write. If it does not exist, it will be created on the fly.
    table: <table>
    # When transferring DynamoDB sources to DynamoDB targets (such as other DynamoDB tables or Alternator tables),
    # the migrator supports transferring live changes occurring on the source table after transferring an initial
    # snapshot.
    # Please see the documentation page “Stream Changes” for more details about this option.
    streamChanges: false

Where ``<table>`` should be replaced with your specific value.

Additionally, you can also set the following optional properties:

.. code-block:: yaml

  target:
    # ... same as above

    # Connect to a custom endpoint. Mandatory if writing to ScyllaDB Alternator.
    endpoint:
      # If writing to ScyllaDB Alternator, prefix the hostname with 'http://'.
      host: <host>
      port: <port>

    # AWS availability region:
    region: <region>

    # Authentication credentials:
    credentials:
      accessKey: <access-key>
      secretKey: <secret-key>

    # Throttling settings, set based on your database write capacity units (or wanted capacity).
    # By default, for provisioned tables we use the configured write capacity units, and for on-demand tables we use the value 40000.
    writeThroughput: 1

    # Can be between 0.1 and 1.5, inclusively.
    # 0.5 represents the default write rate, meaning that the job will attempt to consume half of the write capacity of the table.
    # If you increase the value above 0.5, spark will increase the request rate; decreasing the value below 0.5 decreases the write request rate.
    # (The actual write rate will vary, depending on factors such as whether there is a uniform key distribution in the DynamoDB table.)
    throughputWritePercent: 1.0

    # When streamChanges is true, skip the initial snapshot transfer and only stream changes.
    # This setting is ignored if streamChanges is false.
    skipInitialSnapshotTransfer: false

Where ``<host>``, ``<port>``, ``<region>``, ``<access-key>``, and ``<secret-key>`` are replaced with your specific values.

The Migrator also supports advanced AWS authentication options such as using `AssumeRole <https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_cross-account-with-roles.html>`_. Please read the :ref:`configuration reference <aws-authentication>` for more details.
