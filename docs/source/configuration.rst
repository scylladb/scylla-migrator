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
  # Savepoints configuration. Required for resumable migrations.
  # Omit it for MySQL migrations; MySQL savepoints are not supported.
  savepoints:
    # ...
  # Validator configuration. Required only if the app is executed in validation mode.
  validation:
    # ...
  # Optional - Used internally
  skipTokenRanges: []
  # Optional - Used internally
  skipSegments: []

These top-level properties are documented in the following sections (except ``skipTokenRanges`` and ``skipSegments``, which are used internally).

------
Source
------

The ``source`` property describes the type of data to read from. It must be an object with a field ``type`` defining the type of source, and other fields depending on the type of source.

Valid values for the source ``type`` are:

- ``cassandra`` for a CQL-compatible source (Apache Cassandra or ScyllaDB).
- ``parquet`` for a dataset stored using the Parquet format.
- ``dynamodb`` for a DynamoDB source.
- ``alternator`` for a ScyllaDB Alternator source.
- ``dynamodb-s3-export`` for a DynamoDB table exported to S3.
- ``aerospike`` for an Aerospike source.

The following subsections detail the schema of each source type.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
CQL-Compatible Source (Apache Cassandra, ScyllaDB)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A source of type ``cassandra`` can be used together with a target of type ``cassandra`` only.

.. code-block:: yaml

  source:
    type: cassandra
    # Host name of one or more nodes of your database cluster. Use a YAML list
    # (e.g. [node-1, node-2, node-3]) or a comma-separated string to give several
    # contact points (DNS names or IPs), so the migrator can still connect if one
    # of them is down. A DNS name counts as a single contact point even if it
    # resolves to several addresses, so list distinct per-node names.
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
    # Preserve TTLs and WRITETIMEs of cells in the source database. Frozen collections
    # (frozen<list/map/set>) are stored as a single cell and ARE supported. By default this
    # option is *incompatible* with non-frozen (multi-cell) collections (lists, maps, sets),
    # whose elements each carry their own TTL/WRITETIME (see preserveCollectionTimestamps).
    preserveTimestamps: true
    # Opt-in: also preserve the per-element TTL/WRITETIME of non-frozen (multi-cell) collection
    # columns. Each element's original TTL/WRITETIME is re-applied via collection appends after the
    # base row write. Requires a source that can read collection-element metadata: Cassandra 5.0+
    # (WRITETIME(col)/TTL(col)) or ScyllaDB 2026.2+ (per-element subscript WRITETIME(col[key]),
    # auto-detected). Supported for non-frozen sets and maps whose element/key type has a
    # well-defined order (text/ascii/varchar and integer types); non-frozen lists and other
    # key/element types remain unsupported. Static non-frozen collections are also unsupported:
    # such tables are rejected before the migration starts. On a ScyllaDB 2026.2+ source, tables
    # whose primary key includes a frozen tuple, UDT or collection are rejected too (the Cassandra
    # 5.0+ path has no such limit).
    # Elements are restored by update: the group with the oldest WRITETIME is written with
    # col = ? (whose collection tombstone, one microsecond older, removes target elements older
    # than every source element, e.g. stale or target-only entries from an earlier run), the rest
    # with col = col + ?. Target elements NEWER than every source element are kept (last write
    # wins), and a null/empty source collection leaves the target collection untouched. Very large
    # non-frozen collections are expanded into up to one update per distinct element TTL/WRITETIME,
    # which can be memory- and throughput-intensive. So that every write pass reads the same
    # snapshot, the source is read, cached (executor memory, spilling to spark.local.dir) and
    # written in batches: one batch of source splits (a Parquet restore: a batch of files of about
    # that many splits' size) at a time, its cache released and its progress saved in a savepoint
    # before the next one. A batch is spark.default.parallelism splits by default (one wave of
    # tasks); set -Dscylla.migrator.collectionBatchPartitions=<n> on the driver to change it (a
    # non-positive value writes the whole input as one batch). Executor local disk must hold about
    # one batch of source data plus its per-element metadata, so raise splitCount for large tables
    # to keep batches small. An interrupted run resumes from the last completed batch.
    # Has no effect unless preserveTimestamps is also true.
    #
    # Source-quiescence is REQUIRED. On the ScyllaDB 2026.2+ subscript path, collection elements are
    # listed by a base scan and their metadata read by per-row point reads (map values are re-read in
    # the same point read, so a value always carries its own cell's metadata). A concurrent write in
    # between can still: (a) delete an element -> it is dropped from the migrated row and logged;
    # (b) add an element -> it is never queried and is silently absent (it is not in the base scan,
    # so it is not counted in the dropped-element warning). On the Cassandra 5.0+ array path values
    # and metadata come from one scan, so neither applies. Stop writes to the source before
    # migrating.
    #
    # TTL note: TTLs are read as remaining seconds and re-applied on write, so long-running migrations
    # (and validation) see the clock advance between read and write/compare; use validation's
    # ttlToleranceMillis to absorb this drift. A short-lived element can even expire on the source
    # between the read and the append, in which case it is still written to the target with its
    # remaining-TTL-at-read-time and briefly reappears there. Prefer migrating tables whose collection
    # TTLs are comfortably longer than the expected job duration. CQL exposes no TTL for a row
    # marker, so a row inserted USING TTL whose non-key columns are all null or empty (e.g. an empty
    # collection) is written to the target without a TTL.
    #
    # Validation compares per-element collection TTL/WRITETIME when both endpoints can read them:
    # Cassandra 5.0+ in the scan, ScyllaDB 2026.2+ with per-element point lookups after the scan
    # (ceil(n / 100) synchronous queries per collection per row on each ScyllaDB side, so expect
    # validation against ScyllaDB to be slower, and values and metadata to be read at slightly
    # different times: stop writes first). Otherwise (an older server, or a ScyllaDB endpoint whose
    # primary key includes a frozen tuple, UDT or collection) non-frozen collection columns are
    # compared by VALUE only. copyMissingRows restores element TTL/WRITETIME of missing rows when
    # the SOURCE can read them, and is rejected otherwise (re-run the migration to converge instead).
    # Defaults to false.
    preserveCollectionTimestamps: false
    # Number of splits to use - this should be at minimum the amount of cores
    # available in the Spark cluster, and optimally more; higher splits will lead
    # to more fine-grained resumes. Aim for 8 * (Spark cores).
    splitCount: 256
    # Number of connections to use to the CQL-compatible database (Apache Cassandra, ScyllaDB) when copying
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

.. _config-cassandra-cloud-source:

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
DataStax Astra (Cloud Secure Connect bundle)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Set the ``cloud`` block to migrate from a managed cluster (DataStax Astra DB) that is reached
through an SNI proxy. The bundle carries the contact points, the local datacenter and all the TLS
material; therefore the ``host``, ``port``, ``localDC`` and ``sslOptions`` fields **must not** be
set in the same source — the migrator rejects the configuration if they are. Authentication
credentials are not embedded in the bundle and must still be supplied via ``credentials``.

.. code-block:: yaml

  source:
    type: cassandra
    cloud:
      # Path or URL to the Astra secure-connect bundle (zip).
      # The path must be readable from the Spark driver and from every Spark executor that
      # opens a CQL session. Accepted forms:
      #   - Absolute filesystem path (auto-converted to file:// URL at runtime):
      #       /opt/migrator/secure-connect-mycluster.zip
      #   - https:// URL reachable from every node
      #   - Bare filename (for bundles distributed via spark-submit --files):
      #       secure-connect-mycluster.zip
      #   - s3:// or s3a:// URL (works in standard Spark environments; for maximum
      #     reliability prefer --files instead — see below)
      secureBundlePath: /opt/migrator/secure-connect-mycluster.zip
    credentials:
      username: <client-id>
      password: <client-secret>
    keyspace: <keyspace>
    table: <table>
    consistencyLevel: LOCAL_QUORUM
    preserveTimestamps: true
    splitCount: 256
    connections: 8
    fetchSize: 1000

**Using ``--files`` for S3-hosted bundles** (recommended): instead of placing the ``s3://``
URL directly in ``secureBundlePath``, distribute the bundle through Spark's ``--files``
mechanism and refer to it by bare filename:

.. code-block:: bash

  spark-submit --files s3://my-bucket/secure-connect-prod.zip \
    --class com.scylladb.migrator.Migrator migrator.jar

.. code-block:: yaml

  cloud:
    secureBundlePath: secure-connect-prod.zip

Spark downloads the file once per executor before any task runs, so the connector always
finds a local copy — no dependency on Hadoop URL handlers or repeated S3 fetches.

This setting is also accepted on a Cassandra target. See :ref:`config-cassandra-cloud-target`.

.. _config-cassandra-client-routes-source:

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
ScyllaDB Cloud private endpoints (PrivateLink, Private Service Connect)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Set the ``clientRoutes`` block to reach a ScyllaDB Cloud cluster through a private endpoint.
The driver reads the ``system.client_routes`` table for the listed connection IDs and connects to
every node through the private endpoint address and port published for it, so the Spark driver
and executors never need to reach the node IP addresses. Client routes require ScyllaDB Cloud
(ScyllaDB 2026.1 or later).

- ``host`` and ``port`` are the private endpoint address and port you were given for the cluster.
  The port is the one of the private endpoint, not necessarily 9042 (the usual port without client
  routes). They are only used for the initial connection.
- When you have several private endpoint addresses (for example one per availability zone), list
  them all in ``host``, as a YAML list or comma-separated; they all use ``port``. The migrator tries
  all of them for the initial connection, so it still connects while one of them is unavailable.
- ``localDC`` is optional when the listed connection IDs only route the nodes of one datacenter:
  that datacenter is then used. Set it when they route several datacenters.
- ``cloud`` must not be set.
- ``sslOptions`` can be combined with ``clientRoutes``; the driver then uses the TLS port
  published for each route. Enable it when the private connection only accepts TLS.
- ``connectionAddr`` replaces the address published for a connection ID. Set it to the endpoint IP
  address when the published DNS name does not resolve inside your VPC, for example on GCP Private
  Service Connect or on AWS without private DNS enabled on the VPC endpoint.

A cluster can have several connection IDs: one per availability zone of each datacenter, across
several datacenters. The connection IDs are shown in the ScyllaDB Cloud console with the private
connection details. List every connection ID the migrator may use. Together, the listed IDs must
route every node of the local datacenter, so include all the connection IDs that serve it. Listing
the IDs of other datacenters is harmless when ``localDC`` is set: the migrator only connects to
nodes of the local datacenter.

When the migrator first connects, the Spark connector checks that every node of the local
datacenter has a usable route (one that publishes the port the driver needs, ``tls_port`` when TLS
is enabled) for one of the listed connection IDs, and fails with an error naming the uncovered
nodes otherwise. When a node is
published under several of the listed connection IDs, the driver uses one of those routes; the
order of ``endpoints`` does not choose which.

.. code-block:: yaml

  source:
    type: scylla
    # Private endpoint address(es) used for the initial connection, as a YAML list or
    # comma-separated, e.g. [<private-endpoint-address-az1>, <private-endpoint-address-az2>]
    host: <private-endpoint-address>
    # Port of the private endpoint
    port: <private-endpoint-port>
    # Optional with clientRoutes when the connection IDs route a single datacenter
    localDC: <datacenter>
    credentials:
      username: <username>
      password: <pass>
    clientRoutes:
      endpoints:
        # Connection IDs shown in the ScyllaDB Cloud console, e.g. one per availability zone
        - connectionId: <connection-id-dc1-az1>
        - connectionId: <connection-id-dc1-az2>
        # Optional - hostname or IP address (without port) that replaces the address
        # published by ScyllaDB Cloud for this connection ID, e.g. the endpoint IP address
        - connectionId: <connection-id-dc2-az1>
          connectionAddr: <endpoint-ip-address>
      # Optional - advanced shard awareness: the driver picks each connection's source port
      # to reach a given shard. ScyllaDB Cloud private endpoints support it, but it is off by
      # default on the client side: set it to true on ScyllaDB Cloud. Leave it false for a
      # proxy that does not preserve the client source port (Proxy Protocol v2). Default: false.
      # Requests are sent to the right shard either way (basic shard awareness).
      shardAwareness: true
    keyspace: <keyspace>
    table: <table>
    consistencyLevel: LOCAL_QUORUM
    preserveTimestamps: true
    splitCount: 256
    connections: 8
    fetchSize: 1000

This setting is also accepted on a Scylla target. See :ref:`config-scylla-client-routes-target`.

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

A source of type ``dynamodb`` can be used together with a target of type ``dynamodb``, ``alternator``, or ``dynamodb-s3-export``.

.. code-block:: yaml

  source:
    type: dynamodb
    # Name of the source table to read. The table must already exist.
    table: <table>
    # Optional - Connect to a custom endpoint such as DynamoDB Local or LocalStack.
    endpoint:
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

^^^^^^^^^^^^^^^^^
Alternator Source
^^^^^^^^^^^^^^^^^

A source of type ``alternator`` can be used together with a target of type ``dynamodb`` or ``alternator``.

.. code-block:: yaml

  source:
    type: alternator
    table: <table>
    endpoint:
      # Alternator endpoints must include the protocol prefix.
      host: http://<host>
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
    throughputReadPercent: 1.0
    # Optional - At most how many tasks per Spark executor? The default is to use the same as 'scanSegments'.
    maxMapTasks: 1
    # Optional - Remove consumed capacity headers from requests. Defaults to true for Alternator.
    removeConsumedCapacity: true
    # Optional - Alternator-specific routing / client tuning.
    datacenter: dc1
    rack: rack1
    activeRefreshIntervalMs: 1000
    idleRefreshIntervalMs: 60000
    compression: false
    optimizeHeaders: false
    maxConnections: 400
    connectionMaxIdleTimeMs: 600000
    connectionTimeToLiveMs: 0
    connectionAcquisitionTimeoutMs: 10000
    connectionTimeoutMs: 15000
    maxItemsPerBatch: 100
    # Optional - Set to false to send every request to 'endpoint' with the plain AWS SDK client
    # instead of discovering and load balancing over the nodes. Required behind a private
    # endpoint, see below. Default: true.
    loadBalancing: true

.. _config-alternator-private-endpoint:

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Alternator through a private endpoint (PrivateLink, Private Service Connect)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

By default, the migrator uses the Alternator load-balancing client, which asks the ``endpoint``
for the list of nodes (``/localnodes``) and then sends requests to the nodes directly. Behind a
ScyllaDB Cloud private endpoint those node addresses are not reachable. The Alternator client in
use has no support for private endpoints yet, so set:

- ``endpoint`` to the private endpoint address and its port that reaches all nodes, with
  ``https://`` when the private connection only accepts TLS;
- ``loadBalancing: false``, so that every request goes to that endpoint, whose load balancer spreads
  the requests over the nodes.

The settings that only the load-balancing client applies (``datacenter``, ``rack``, the refresh
intervals, ``compression``, ``optimizeHeaders`` and the connection pool settings) cannot be combined
with ``loadBalancing: false``. ``maxItemsPerBatch`` and ``removeConsumedCapacity`` still apply.

.. code-block:: yaml

  source:
    type: alternator
    table: <table>
    endpoint:
      host: https://<private-endpoint-address>
      port: <private-endpoint-port>
    loadBalancing: false

The same applies to an Alternator target.

^^^^^^^^^^^^^^^^^^^^^^^^^
DynamoDB S3 Export Source
^^^^^^^^^^^^^^^^^^^^^^^^^

A source of type ``dynamodb-s3-export`` can be used together with a target of type ``dynamodb`` or ``alternator``.

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

^^^^^^^^^^^^^^^^
Aerospike Source
^^^^^^^^^^^^^^^^

An Aerospike source migrates to a CQL-compatible target. Use JDK 17 or later on
the driver and every executor. Create the destination keyspace and table before
starting the migration.

The required settings are ``hosts`` (a list of seed hosts), ``namespace``,
``set``, ``schema``, and ``keyMode``. The namespace and set must be nonempty.
``port`` defaults to 3000 for seeds without an inline port.

For records with stored string keys, this configuration selects four bins and
exports the TTL and generation as ordinary metadata columns:

.. code-block:: yaml

  source:
    type: aerospike
    hosts:
      - aerospike.example.com
    port: 3000
    namespace: test
    set: users
    keyMode: string
    schema:
      name: string
      age: long
      tags: list<string>
      attrs: map<string,long>
    invalidRecordHandling: skip
    preserveTTL: true
    preserveGeneration: true

  target:
    type: scylla
    host: scylla.example.com
    port: 9042
    keyspace: migration
    table: users
    consistencyLevel: LOCAL_QUORUM
    connections: 16
    stripTrailingZerosForDecimals: false

In an existing ``migration`` keyspace, a matching table is:

.. code-block:: cql

  CREATE TABLE migration.users (
    aero_key text PRIMARY KEY,
    name text,
    age bigint,
    tags frozen<list<text>>,
    attrs frozen<map<text, bigint>>,
    aero_ttl int,
    aero_generation int
  );

**Schema and record identity**

``schema`` is a required mapping from bin names to types. Only declared bins are
migrated; undeclared bins are omitted and missing declared bins remain null.
An explicit ``schema: {}`` selects only the generated key and enabled metadata.
Automatic inference, ``schemaSampleSize``, ``schemaDiscoveryStrategy``, and a
separate ``bins`` filter are not supported.

Supported types are ``string``/``text``, ``long``/``bigint``/``int``/``integer``,
``double``, ``boolean``/``bool``, and ``binary``/``blob``, plus nested ``list<T>``
and ``map<K,V>``. Integer aliases all produce Spark long values. Boolean types
require native Boolean bins; integer-backed flags must use long or string.
A string schema selects a textual representation: binary values use hex,
collections use Java-style punctuation, and GeoJSON uses JSON text. Collection
text does not promise parseable or round-trip serialization.

``keyMode`` is required independently of ``schema``:

.. list-table:: Record-key modes
   :header-rows: 1
   :widths: 15 45 40

   * - Mode
     - Generated key
     - Required target CQL type
   * - ``string``
     - Original stored string key
     - ``text`` or ``varchar``
   * - ``integer``
     - Original stored integer key
     - ``bigint``
   * - ``binary``
     - Original stored binary key
     - ``blob``
   * - ``digest``
     - Lowercase 40-character hex digest
     - ``text`` or ``varchar``

Stored-key modes require records written with ``sendKey=true``. This is an
Aerospike writer setting, not a migration read option. Missing or mismatched
stored keys skip the record in both invalid-record modes, without coercion or
digest fallback. Digest mode supports mixed key types and records without stored
keys; it emits the digest for every record instead of the original key.

The generated key column defaults to ``aero_key``. ``keyColumn``, ``ttlColumn``,
and ``generationColumn`` customize generated names to avoid collisions with bins.
Only enabled generated columns reserve their names. Top-level ``renames`` can
map emitted columns to destination names.

**Target schema and invalid records**

Every emitted column must exist in the target table. The generated record key
must belong to its primary key with the exact CQL type listed above. All other
target primary-key columns must also be supplied and non-null. When writing
static columns, the generated key must belong to the partition key so distinct
records cannot overwrite shared static values.

Native lists require CQL lists to preserve order and duplicates; maps require
compatible CQL maps. Nested CQL collections must be frozen. Null collection
members, converted map-key collisions, and empty non-frozen target collections
are invalid field values. Use frozen collections when empty collections must be
preserved. Incompatible source/target schema mappings fail the job.

``invalidRecordHandling`` controls source conversion and per-record destination
validation, except for list-budget overflow described below:

- ``skip`` (default) omits the entire record when a declared value is invalid.
- ``"null"`` retains valid fields and replaces each invalid top-level field with
  null where the destination permits it. Quote ``"null"`` in YAML; unquoted null
  is not a valid policy. A bad collection member invalidates the whole field.

Numeric overflow, rounding, and truncation are invalid values, including at the
target CQL boundary. Double-to-decimal conversion must preserve the source's
exact mathematical value: 0.1 is rejected, while 0.5 is valid. Missing or invalid
record keys and invalid or null target primary-key values skip the whole record
in either mode. Configuration, connection, exhausted scan retries, and write
errors remain fatal. Null writes follow the destination's existing settings;
when nulls are ignored, an older destination cell may remain unchanged.

``listOverflowHandling`` independently selects ``skip`` (default), ``nullify``,
or ``empty`` when retained top-level non-frozen lists exceed the shared budget
of 131,072 outer elements in one row INSERT. Frozen lists and nested frozen
contents do not add to this count. After ordinary validation, ``skip`` omits the
record; ``nullify`` replaces every contributing list with null; ``empty`` binds
every contributing list as empty. Both replacement modes preserve other surviving
fields and leave originally null or already nulled fields null. The budget resets
for each row. Source empty lists still follow ordinary validation. Empty bindings
clear those destination columns under normal write semantics, even when nulls
are ignored; non-frozen CQL collections do not retain a distinct empty value.
Null bindings can leave older values when null writes are ignored.

**Metadata and restarts**

``preserveTTL`` and ``preserveGeneration`` default to false. When enabled, they
add ``aero_ttl`` and ``aero_generation`` columns, respectively, which require
compatible target columns (normally CQL ``int``). TTL is remaining seconds at
read time, with -1 for no expiration; generation is the record's version counter.
These are ordinary values: they do not apply per-record target write TTLs or
preserve write timestamps or conflict resolution.

Aerospike scans have at-least-once semantics: retries can replay emitted records.
Durable resume is not supported. Omit ``savepoints``; configured savepoints are
ignored for this source. A failed job can leave written rows in the destination,
and a restart scans from the beginning without rolling them back. Use a target
that tolerates repeated writes.

**Authentication and TLS**

Set ``AEROSPIKE_USERNAME`` and ``AEROSPIKE_PASSWORD`` as a nonempty pair on the
driver and every executor to use environment credentials. They take precedence
over ``source.credentials``. An empty value or a partial pair fails before
connection. Configured credentials are used only when both environment variables
are absent. When the driver selects environment credentials, configured
credentials are not broadcast; every executor must supply its own nonempty pair.
When both variables are absent on the driver, configured credentials are sent to
executors in a Spark broadcast.

TLS is enabled only by ``source.tlsName``. This verification name applies to every
seed and overrides TLS names in seed strings. Inline seed TLS names alone do not
enable TLS: a seed with an inline TLS name and no top-level ``source.tlsName``
fails configuration validation before connecting. Set ``source.tlsName`` to enable
TLS. Per-host TLS activation and distinct seed verification names are not supported.

TLS uses the JVM default trust store. Configure a custom trust store on both the
driver and executors with ``-Djavax.net.ssl.trustStore``.

------
Target
------

The ``target`` property describes the type of data to write. It must be an object with a field ``type`` defining the type of target, and other fields depending on the type of target.

Valid values for the target ``type`` are:

- ``cassandra`` for a CQL-compatible target (Apache Cassandra or ScyllaDB).
- ``dynamodb`` for a DynamoDB target.
- ``alternator`` for a ScyllaDB Alternator target.

The following subsections detail the schema of each target type.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
CQL-Compatible Target (Apache Cassandra, ScyllaDB)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: yaml

  target:
    type: cassandra
    # Host name of one or more nodes of your target database cluster. Use a YAML list
    # (e.g. [node-1, node-2, node-3]) or a comma-separated string to give several
    # contact points (DNS names or IPs), so the migrator can still connect if one
    # of them is down. A DNS name counts as a single contact point even if it
    # resolves to several addresses, so list distinct per-node names.
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
    # Number of connections to use to the CQL-compatible database (Apache Cassandra, ScyllaDB) when copying
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
    # Optional - writetime in microseconds since the Unix epoch (16 digits; sample 1640998861000000 is Saturday, January 1, 2022 1:01:01 AM UTC)
    writeWritetimestampInuS: 1640998861000000
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

.. _config-cassandra-cloud-target:

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
DataStax Astra (Cloud Secure Connect bundle)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Same semantics as :ref:`config-cassandra-cloud-source` but on the target side. ``host``, ``port``,
``localDC`` and ``sslOptions`` must not be set when ``cloud`` is provided. The same
``secureBundlePath`` forms (absolute path, ``https://``, bare filename via ``--files``, ``s3://``)
are accepted.

.. code-block:: yaml

  target:
    type: cassandra
    cloud:
      secureBundlePath: /opt/migrator/secure-connect-mycluster.zip
    credentials:
      username: <client-id>
      password: <client-secret>
    keyspace: <keyspace>
    table: <table>
    consistencyLevel: LOCAL_QUORUM
    connections: 16
    stripTrailingZerosForDecimals: false

.. _config-scylla-client-routes-target:

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
ScyllaDB Cloud private endpoints (PrivateLink, Private Service Connect)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

Same semantics as :ref:`config-cassandra-client-routes-source` but on the target side:
``host``/``port`` point at the private endpoint, ``localDC`` is required only when the listed
connection IDs route several datacenters, and ``cloud`` must not be set.

.. code-block:: yaml

  target:
    type: scylla
    host: <private-endpoint-address-az1>,<private-endpoint-address-az2>
    port: <private-endpoint-port>
    localDC: <datacenter>
    credentials:
      username: <username>
      password: <pass>
    clientRoutes:
      endpoints:
        - connectionId: <connection-id-dc1-az1>
        - connectionId: <connection-id-dc1-az2>
    keyspace: <keyspace>
    table: <table>
    consistencyLevel: LOCAL_QUORUM
    connections: 16
    stripTrailingZerosForDecimals: false


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
    # When transferring DynamoDB sources to DynamoDB-like targets (DynamoDB or Alternator),
    # the migrator supports transferring live changes occurring on the source table after transferring an initial
    # snapshot.
    # Please see the documentation page “Stream Changes” for more details about this option.
    streamChanges: false
    # Optional - When streamChanges is true, skip the initial snapshot transfer and only stream changes.
    # This setting is ignored if streamChanges is false.
    skipInitialSnapshotTransfer: false

^^^^^^^^^^^^^^^^^
Alternator Target
^^^^^^^^^^^^^^^^^

.. code-block:: yaml

  target:
    type: alternator
    # Name of the table to write. If it does not exist, it will be created on the fly.
    table: <table>
    # Alternator endpoints must include the protocol prefix.
    endpoint:
      host: http://<host>
      port: <port>
    # Optional - Throttling settings, set based on your database write capacity units (or wanted capacity).
    writeThroughput: 1
    # Optional - Can be between 0.1 and 1.5, inclusively.
    throughputWritePercent: 1.0
    # Please see the documentation page “Stream Changes” for more details about this option.
    streamChanges: false
    # Optional - When streamChanges is true, skip the initial snapshot transfer and only stream changes.
    skipInitialSnapshotTransfer: false
    # Optional - Remove consumed capacity headers from requests. Defaults to true for Alternator.
    removeConsumedCapacity: true
    # Optional - Alternator-specific routing / client tuning.
    datacenter: dc1
    rack: rack1
    activeRefreshIntervalMs: 1000
    idleRefreshIntervalMs: 60000
    compression: false
    optimizeHeaders: false
    maxConnections: 400
    connectionMaxIdleTimeMs: 600000
    connectionTimeToLiveMs: 0
    connectionAcquisitionTimeoutMs: 10000
    connectionTimeoutMs: 15000
    maxItemsPerBatch: 100
    # Optional - Set to false to send every request to 'endpoint' with the plain AWS SDK client.
    # Required behind a private endpoint, see :ref:`config-alternator-private-endpoint`.
    # Default: true.
    loadBalancing: true

-------
Renames
-------

The optional ``renames`` property lists the item columns to rename along the migration.

.. code-block:: yaml

  renames:
    - from: <source-column-name>
      to: <target-column-name>
    # ... other columns to rename

.. _config-savepoints:

----------
Savepoints
----------

For resumable sources, the migrator is able to :doc:`resume an interrupted migration </resume-interrupted-migration>`.
To achieve this, it stores so-called “savepoints” along the process to remember which data items
have already been migrated and should be skipped when the migration is restarted.

This section must be omitted for MySQL migrations. MySQL savepoints are not supported: Spark JDBC
reads do not expose durable per-range progress for MySQL, and partitioned reads use multiple
independent JDBC statements instead of one resumable snapshot. If a MySQL migration is interrupted,
it must be restarted from the beginning. For large tables, ensure the target schema uses idempotent
writes so that re-running the migration is safe.

.. code-block:: yaml

  savepoints:
    # Where should savepoint configurations be stored? This is a path on the host running
    # the Spark driver - usually the Spark master.
    path: /app/savepoints
    # Interval at which savepoints will be created
    intervalSeconds: 300
    # Optional, enabled by default. Merge saved progress into the launch configuration.
    autoResume: true

For local paths, the top-level ``path`` is a shorthand for an explicit filesystem target:

.. code-block:: yaml

  savepoints:
    intervalSeconds: 300
    target:
      type: filesystem
      path: /app/savepoints

Specify either ``savepoints.path`` or ``savepoints.target``, never both. A structured
``filesystem`` target requires its own nonempty ``target.path``.
For file and cloud destinations, ``intervalSeconds`` must be set explicitly.
For ``target-table`` (also accepted as ``target-database``), it defaults to 300 seconds
when omitted.

For CQL targets (``target.type: scylla`` or its ``cassandra`` alias), savepoints can also be stored in the target database:

.. code-block:: yaml

  savepoints:
    intervalSeconds: 300
    autoResume: true
    target:
      type: target-table
      # Optional. Default: target.keyspace. The keyspace must already exist.
      keyspace: null
      # Optional. Created automatically if it does not exist.
      table: scylla_migrator_savepoints
      # Optional. If omitted, the migrator derives a stable job id from the
      # redacted configuration identity, including renames but excluding progress,
      # savepoints, and validation.
      # Explicit IDs select rows but still require matching configuration identity.
      jobId: null

Explicit ``savepoints.target.keyspace`` and ``savepoints.target.table`` values use
CQL naming rules: only ASCII letters, digits, and underscores are allowed, even
inside quotes. An unquoted simple name such as ``MyKs`` becomes ``myks``.
To preserve case, include the CQL double quotes inside the YAML string:

.. code-block:: yaml

  savepoints:
    target:
      type: target-table
      keyspace: '"MyKs"'
      table: '"MySavepoints"'

When ``savepoints.target.keyspace`` is omitted, the migrator inherits the exact
case of ``target.keyspace`` as used by the data connector.

The ``target-table`` backend supports migration ``target.type: scylla`` and its
``cassandra`` alias.
Metadata writes, resume reads, and seed reads always use ``LOCAL_QUORUM``,
independently of bulk-data consistency.
See :doc:`resume-interrupted-migration` for availability costs and limitations.
The migrator creates the metadata table automatically, but it does not create the keyspace. Each
savepoint is stored as an append-only version in this table:

.. code-block:: cql

  CREATE TABLE IF NOT EXISTS <keyspace>.<table> (
    job_id text,
    epoch_millis bigint,
    sequence bigint,
    schema_version int,
    reason text,
    migrator_version text,
    config_sha256 text,
    config_yaml text,
    PRIMARY KEY ((job_id), epoch_millis, sequence)
  ) WITH CLUSTERING ORDER BY (epoch_millis DESC, sequence DESC);

Run only one active migration per savepoint job. Stop the previous run before
restarting it, and use separate job IDs for separate migrations. Operators must
enforce this restriction: accidental overlap can overwrite progress.

History is append-only and each row contains a full redacted configuration payload.
Monitor metadata table and job partition growth, and the size of ``config_yaml`` as
progress accumulates. Reads use small pages and retain fallback to older usable
savepoints, but history can grow without limit and large payloads can fail database
writes. Paging does not bound individual payload size, stored history, or total scan
work. There is no automatic retention or bounded payload representation.

The ``config_yaml`` column contains redacted savepoint YAML, matching file-backed
savepoints. Automatic resume is enabled by default for every backend. Only progress is merged
into the launch configuration; credentials and other settings come from that live configuration.
Missing or failed lookups warn and continue from the launch configuration. Set
``savepoints.autoResume: false`` to disable lookup. Table payloads retain a SHA-256 digest
of any source WHERE filter to verify dataset identity without storing the cleartext filter.
This unsalted digest allows a reader to confirm guessed filters, so it does not protect
low-entropy filter values. Restrict access to the metadata table and its saved payloads.

The structured ``filesystem`` target accepts only local filesystem paths, including
``file://`` paths. For Hadoop filesystem URIs, use the legacy ``path`` field directly.
For example, Google Cloud Storage and AWS S3 savepoint locations can be written as raw
Hadoop filesystem URIs:

.. code-block:: yaml

  savepoints:
    path: gs://my-bucket/scylla-migrator/savepoints
    intervalSeconds: 300

.. code-block:: yaml

  savepoints:
    path: s3a://my-bucket/scylla-migrator/savepoints
    intervalSeconds: 300

Google Cloud Storage savepoint locations can also be written as a structured GCS
target. The structured form below resolves to
``gs://my-bucket/scylla-migrator/savepoints``:

.. code-block:: yaml

  savepoints:
    intervalSeconds: 300
    target:
      type: gcs
      bucket: my-bucket
      prefix: scylla-migrator/savepoints
      projectId: my-gcp-project
      credentials:
        serviceAccountJsonKeyfile: /path/to/service-account.json

The structured GCS target accepts optional ``projectId`` and ``credentials`` fields.
When ``credentials.serviceAccountJsonKeyfile`` is provided, the migrator configures
the Hadoop GCS connector to use ``SERVICE_ACCOUNT_JSON_KEYFILE`` authentication. If
the configuration file itself is loaded from GCS, Spark/Hadoop runtime configuration
must still contain credentials that can read that file before the migrator can parse
these settings.

AWS S3 savepoint locations can be written either as an ``s3a://`` filesystem URI or
as a structured S3 target. The structured form below resolves to
``s3a://my-bucket/scylla-migrator/savepoints``:

.. code-block:: yaml

  savepoints:
    intervalSeconds: 300
    target:
      type: s3
      bucket: my-bucket
      prefix: scylla-migrator/savepoints
      region: us-east-1

The structured S3 target accepts the same optional ``region`` and ``credentials``
fields used by other AWS-backed migrator settings and applies them to the Hadoop
S3A filesystem used for writing savepoints. If the configuration file itself is
loaded from S3, Spark/Hadoop runtime configuration must still contain credentials
that can read that file before the migrator can parse these settings.

For raw ``gs://`` paths, make sure the Spark runtime includes the Hadoop GCS connector.
When GCS authentication is not configured in the migrator YAML file, provide it through
Spark/Hadoop runtime configuration so it is available to the Spark driver when the
migrator writes savepoints and when it later loads a savepoint file to resume.
For example, when using a service account JSON keyfile with the GCS connector:

.. code-block:: bash

  spark-submit \
    --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
    --conf spark.hadoop.fs.gs.auth.type=SERVICE_ACCOUNT_JSON_KEYFILE \
    --conf spark.hadoop.fs.gs.auth.service.account.json.keyfile=/path/to/key.json \
    ...

Other GCS connector authentication modes, such as Compute Engine service account
credentials on Dataproc, should be configured the same way: as Spark/Hadoop runtime
configuration, not as ``savepoints`` fields.

Savepoint files are written to a temporary sibling path first and then renamed to the
final ``savepoint_*.yaml`` name. On local filesystems this rename is atomic when the
filesystem supports atomic moves. On object stores such as GCS, the exact rename
semantics are provided by the Hadoop connector, so the operation may be implemented as
a copy/delete, but the migrator still avoids writing partial YAML directly to the final
savepoint name.

----------
Validation
----------

The ``validation`` field and its properties are mandatory only when the application is executed in :doc:`validation mode </validate>`.

For custom MySQL join keys, see :ref:`mysql-custom-join-keys`. The source options
``skipSourcePrimaryKeyValidation``, ``verifySourceJoinKeyUniqueness``, and
``acknowledgeUnverifiedJoinKey`` default to ``false`` and affect validation only.

When validating MySQL-to-ScyllaDB migrations, ensure that writes have stopped on both sides before
starting the validator. The current implementation may issue multiple reads over time and therefore
does not provide a single global point-in-time snapshot while the source or target table is still
changing.

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
    # Optional - columns to compare via a content hash for MySQL-to-ScyllaDB validation.
    # Use source-side column names; renames are applied automatically.
    # This reduces Spark-side join/shuffle volume, but the validator still reads the
    # original payload columns from MySQL and ScyllaDB before hashing.
    hashColumns:
      - large_text_column
      - blob_column

.. _aws-authentication:

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
