package com.scylladb.migrator

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder

object CassandraUtils {

/**
 * Prepare a Scylla table for a test. Drop any existing table of the same name and recreates it.
 *
 * @param database   Database session to use
 * @param keyspace   Keyspace name
 * @param name       Name of the table to create
 * @param columnName Function to possible transform the initial name of the columns
 */
def dropAndRecreateTable(database: CqlSession,
                         keyspace: String,
                         name: String,
                         columnName: String => String): Unit = {
val dropTableStatement =
        SchemaBuilder
                .dropTable(keyspace, name)
                .ifExists()
                .build()
    database
            .execute(dropTableStatement)
      .ensuring(_.wasApplied())
val createTableStatement =
        SchemaBuilder
                .createTable(keyspace, name)
                .withPartitionKey("id", DataTypes.TEXT)
                .withColumn(columnName("foo"), DataTypes.TEXT)
                .withColumn(columnName("bar"), DataTypes.INT)
                .build()
    database
            .execute(createTableStatement)
      .ensuring(_.wasApplied())
        }
        }