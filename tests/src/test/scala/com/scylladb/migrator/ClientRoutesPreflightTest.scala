package com.scylladb.migrator

import com.scylladb.migrator.ClientRoutesPreflight.Route

import java.util.UUID

class ClientRoutesPreflightTest extends munit.FunSuite {

  private val dc1Node1 = UUID.fromString("00000000-0000-0000-0000-000000000011")
  private val dc1Node2 = UUID.fromString("00000000-0000-0000-0000-000000000012")
  private val dc2Node1 = UUID.fromString("00000000-0000-0000-0000-000000000021")

  private def evaluate(
    connectionIds: Seq[String],
    routes: Seq[Route],
    localHostIds: Set[UUID] = Set(dc1Node1, dc1Node2)
  ) =
    ClientRoutesPreflight.evaluate(
      side          = "Scylla target",
      localDC       = "dc1",
      connectionIds = connectionIds,
      routes        = routes,
      localHostIds  = localHostIds,
      knownDCs      = Set("dc1", "dc2")
    )

  test("passes when every local node has a route") {
    val result = evaluate(
      Seq("conn-dc1"),
      Seq(Route("conn-dc1", dc1Node1), Route("conn-dc1", dc1Node2))
    )
    assertEquals(result, Right(Nil))
  }

  test("passes with one connection ID per datacenter") {
    val result = evaluate(
      Seq("conn-dc1", "conn-dc2"),
      Seq(Route("conn-dc1", dc1Node1), Route("conn-dc1", dc1Node2), Route("conn-dc2", dc2Node1))
    )
    assertEquals(result, Right(Nil))
  }

  test("fails when a local node has no route") {
    val result = evaluate(Seq("conn-dc1"), Seq(Route("conn-dc1", dc1Node1)))
    result match {
      case Left(error) =>
        assert(error.contains("1 of 2 node(s) in localDC 'dc1'"), error)
        assert(error.contains(dc1Node2.toString), error)
        assert(!error.contains(dc1Node1.toString), error)
      case Right(warnings) => fail(s"Expected an error, got warnings $warnings")
    }
  }

  test("fails when the connection ID matches no route (e.g. a typo)") {
    val result = evaluate(Seq("conn-typo"), Nil)
    assert(result.left.exists(_.contains("2 of 2 node(s)")), result.toString)
    assert(result.left.exists(_.contains("conn-typo")), result.toString)
  }

  test("routes of remote datacenters do not cover local nodes") {
    val result = evaluate(Seq("conn-dc2"), Seq(Route("conn-dc2", dc2Node1)))
    assert(result.isLeft, result.toString)
  }

  test("fails when localDC has no nodes, listing the known datacenters") {
    val result = evaluate(Seq("conn-dc1"), Nil, localHostIds = Set.empty)
    assertEquals(
      result,
      Left("Scylla target: no nodes found in localDC 'dc1'. Known datacenters: dc1, dc2.")
    )
  }

  test("warns about connection IDs without routes when the local DC is covered") {
    val result = evaluate(
      Seq("conn-dc1", "conn-unused"),
      Seq(Route("conn-dc1", dc1Node1), Route("conn-dc1", dc1Node2))
    )
    assertEquals(
      result,
      Right(
        List("Scylla target: connection ID 'conn-unused' has no entries in system.client_routes.")
      )
    )
  }
}
