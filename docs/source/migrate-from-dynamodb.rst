=====================
Migrate from DynamoDB
=====================


This page explains how to fill the ``source`` and ``target`` properties of the `configuration file </configuration>`_ to migrate data:

- from a DynamoDB table, a ScyllaDB’s Alternator table, or a `DynamoDB S3 export <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.HowItWorks.html>`_,
- to a DynamoDB table or a ScyllaDB’s Alternator table.

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

The Migrator also supports advanced AWS authentication options such as using `AssumeRole <https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_cross-account-with-roles.html>`_. Please read the `configuration reference </configuration#aws-authentication>`_ for more details.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Reading a DynamoDB S3 Export
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To read the content of a table exported to S3, use the ``source`` type ``dynamodb-s3-export``. Here is a minimal source configuration:

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
        - ...
      # See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_KeySchemaElement.html
      keySchema:
        - name: <key-name>
          type: <key-type>
        - ...


Additionally, you can also provide the following optional properties:

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

The Migrator also supports advanced AWS authentication options such as using `AssumeRole <https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_cross-account-with-roles.html>`_. Please read the `configuration reference </configuration#aws-authentication>`_ for more details.

---------------------------
Configuring the Destination
---------------------------

The migration ``target`` can be DynamoDB or Alternator. In both cases, we use the configuration type ``dynamodb`` in the configuration. Here is a minimal ``target`` configuration to write to DynamoDB or Alternator:

.. code-block:: yaml

  target:
    type: dynamodb
    # Name of the table to write. If it does not exist, it will be created on the fly.
    table: <table>
    # Split factor for reading/writing. This is required for Scylla targets.
    scanSegments: 1
    # Throttling settings, set based on your database capacity (or wanted capacity)
    readThroughput: 1
    # Can be between 0.1 and 1.5, inclusively.
    # 0.5 represents the default read rate, meaning that the job will attempt to consume half of the read capacity of the table.
    # If you increase the value above 0.5, spark will increase the request rate; decreasing the value below 0.5 decreases the read request rate.
    # (The actual read rate will vary, depending on factors such as whether there is a uniform key distribution in the DynamoDB table.)
    throughputReadPercent: 1.0
    # At most how many tasks per Spark executor?
    maxMapTasks: 1
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

    # Connect to a custom endpoint. Mandatory if writing to Scylla Alternator.
    endpoint:
      # If writing to Scylla Alternator, prefix the hostname with 'http://'.
      host: <host>
      port: <port>

    # AWS availability region:
    region: <region>

    # Authentication credentials:
    credentials:
      accessKey: <access-key>
      secretKey: <secret-key>

    # When streamChanges is true, skip the initial snapshot transfer and only stream changes.
    # This setting is ignored if streamChanges is false.
    skipInitialSnapshotTransfer: false

Where ``<host>``, ``<port>``, ``<region>``, ``<access-key>``, and ``<secret-key>`` are replaced with your specific values.

The Migrator also supports advanced AWS authentication options such as using `AssumeRole <https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_cross-account-with-roles.html>`_. Please read the `configuration reference </configuration#aws-authentication>`_ for more details.
