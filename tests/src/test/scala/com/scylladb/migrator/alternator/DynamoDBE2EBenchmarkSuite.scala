package com.scylladb.migrator.alternator

import com.scylladb.migrator.{ E2E, Integration }
import org.junit.experimental.categories.Category

import scala.concurrent.duration._

/** Abstract base class for DynamoDB end-to-end throughput benchmarks.
  *
  * Centralizes munitTimeout and rowCount parsing for all DynamoDB E2E benchmarks.
  */
@Category(Array(classOf[Integration], classOf[E2E]))
abstract class DynamoDBE2EBenchmarkSuite extends MigratorSuiteWithDynamoDBLocal {

  override val munitTimeout: Duration = 60.minutes

  protected val rowCount: Int = sys.props.getOrElse("e2e.ddb.rows", "500000").toIntOption
    .getOrElse(throw new IllegalArgumentException("e2e.ddb.rows must be a valid integer"))
}
