=======================
Configuration Reference
=======================

------------------
AWS Authentication
------------------

When reading from DynamoDB or S3, or when writing to DynamoDB, the communication with AWS can be configured with the properties ``credentials``, ``endpoint``, and ``region`` in the configuration:

.. code-block:: yaml

  credentials:
    accessKey: <access-key>
    secretKey: <secret-key>
  # Optional AWS endpoint configuration
  endpoint:
    host: <host>
    port: <port>
  # Optional AWS availability region, required if you use a custom endpoint
  region: <region>

Additionally, you can authenticate with `AssumeRole <https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_cross-account-with-roles.html>`_. In such a case, the ``accessKey`` and ``secretKey`` are the credentials of the user whose access to the resource (DynamoDB table or S3 bucket) has been granted via a “role”, and you need to add the property ``assumeRole`` as follows:

.. code-block:: yaml

  credentials:
    accessKey: <access-key>
    secretKey: <secret-key>
    assumeRole:
      arn: <role-arn>
      # Optional session name to use. If not set, we use 'scylla-migrator'.
      sessionName: <role-session-name>
  # Note that the region is mandatory when you use `assumeRole`
  region: <region>
