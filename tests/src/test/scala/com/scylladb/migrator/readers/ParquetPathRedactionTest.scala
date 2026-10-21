package com.scylladb.migrator.readers

/** Unit coverage for [[Parquet.redactPathForLog]]. Source paths reach the logs on every Parquet
  * read, and object-store URIs routinely carry credentials in the userinfo component or a signed
  * token in the query string. The helper must fail CLOSED: anything it cannot prove credential-free
  * is redacted, so a malformed or unusual URI never leaks a secret into a log file.
  */
class ParquetPathRedactionTest extends munit.FunSuite {

  test("plain paths and URIs are logged as-is") {
    for (
      path <- Seq(
                "/tmp/data/part-0.parquet",
                "file:///home/user/data",
                "s3a://bucket/prefix/table",
                "gs://bucket/prefix"
              )
    )
      assertEquals(Parquet.redactPathForLog(path), path, s"expected '$path' to be logged verbatim")
  }

  test("credentials in the userinfo component are redacted") {
    for (
      path <- Seq(
                "s3a://AKIAEXAMPLE:secretkey@bucket/prefix",
                "//user:secret@host/prefix" // schemeless but still parses: must not be echoed
              )
    )
      assert(
        !Parquet.redactPathForLog(path).contains("secret"),
        s"expected credentials in '$path' to be redacted, got '${Parquet.redactPathForLog(path)}'"
      )
  }

  test("a signed query string or fragment is redacted") {
    val signed = "https://bucket.s3.amazonaws.com/key?X-Amz-Signature=deadbeef&X-Amz-Expires=60"
    assert(
      !Parquet.redactPathForLog(signed).contains("deadbeef"),
      s"expected the signature to be redacted, got '${Parquet.redactPathForLog(signed)}'"
    )
    assert(!Parquet.redactPathForLog("s3a://bucket/key#tok=secret").contains("secret"))
  }

  test("an unparseable path carrying credential markers is redacted, not echoed") {
    // `[` is illegal in a URI, so parsing throws; the fail-closed branch must still redact.
    val malformed = "s3a://user:secret@bucket/pre[fix"
    assertEquals(Parquet.redactPathForLog(malformed), "<redacted-path>")
    // ...while a malformed but plainly credential-free local path stays readable for diagnostics.
    assertEquals(Parquet.redactPathForLog("/tmp/pre[fix"), "/tmp/pre[fix")
  }
}
