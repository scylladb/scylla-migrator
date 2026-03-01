package com.scylladb.migrator.scylla

import com.scylladb.migrator.{ E2E, Integration }
import org.junit.experimental.categories.Category

import scala.concurrent.duration._

/** Abstract base class for end-to-end throughput benchmarks (1M+ rows).
  *
  * Same structure as [[BenchmarkSuite]] but tagged with E2E for separate invocation via Makefile
  * targets.
  *
  * Row counts are configurable via system properties (set in Makefile):
  *   - `e2e.cql.rows` — row count for CQL-based benchmarks (default: 5000000)
  */
@Category(Array(classOf[Integration], classOf[E2E]))
abstract class E2EBenchmarkSuite(sourcePort: Int)
    extends MigratorSuite(sourcePort)
    with ThroughputBenchmarkSupport {

  override val munitTimeout: Duration = 60.minutes

  protected val rowCount: Int = sys.props.getOrElse("e2e.cql.rows", "5000000").toIntOption
    .getOrElse(throw new IllegalArgumentException("e2e.cql.rows must be a valid integer"))
}
