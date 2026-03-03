package com.scylladb.migrator.writers

/** Poll a condition at regular intervals until it returns true or the timeout expires.
  * @throws AssertionError
  *   if the timeout expires before the condition is met
  */
object Eventually {
  def apply(
    timeoutMs: Long = 10000,
    intervalMs: Long = 200
  )(condition: => Boolean)(message: => String): Unit = {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (!condition) {
      if (System.currentTimeMillis() >= deadline)
        throw new AssertionError(s"Timed out after ${timeoutMs}ms: ${message}")
      Thread.sleep(intervalMs)
    }
  }
}
