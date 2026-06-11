package com.scylladb.migrator.readers.jdbc

class JdbcFetchSizeTest extends munit.FunSuite {

  test("MaxFetchSize and RecommendedMaxFetchSize have the documented values") {
    assertEquals(JdbcFetchSize.MaxFetchSize, 100000)
    assertEquals(JdbcFetchSize.RecommendedMaxFetchSize, 10000)
  }

  test("aboveRecommendedWarning interpolates fetchSize and recommended ceiling") {
    val message = JdbcFetchSize.aboveRecommendedWarning(50000)
    assert(message.contains("fetchSize (50000)"))
    assert(message.contains(s"recommended maximum of ${JdbcFetchSize.RecommendedMaxFetchSize}"))
    assert(message.contains("1000-10000"))
  }
}
