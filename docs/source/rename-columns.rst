==============
Rename Columns
==============

The migration requires the target table (if it already exists) to have the same schema as the source table. For convenience, it is possible to rename some item columns along the way.

Indicate in the migration configuration which columns to rename with the ``renames`` top-level property. For instance, to rename the column ``foo`` into ``bar``, and ``xxx`` into ``yyy``, add the following configuration:

.. code-block:: yaml

  renames:
    - from: foo
      to: bar
    - from: xxx
      to: yyy
