package skunk.sharp.dsl

import skunk.sharp.dsl.*

import java.util.UUID

object ValuesSuite {
  case class User(id: UUID, email: String, age: Int)
}

class ValuesSuite extends munit.FunSuite {
  import ValuesSuite.*

  private val users = Table.of[User]("users")

  test("Values.of + .alias renders `(VALUES (…), (…)) AS \"alias\" (col, …)` in FROM") {
    val lookup = Values.of(
      (id = 1, label = "alpha"),
      (id = 2, label = "beta")
    ).alias("lookup")

    val af = lookup.select.compile.af

    assertEquals(
      af.fragment.sql,
      """SELECT "id", "label" FROM (VALUES ($1, $2), ($3, $4)) AS "lookup" ("id", "label")"""
    )
  }

  test("Values-as-subquery used as insert source via .from — no `.alias` needed in this position") {
    val af = users.insert.from(
      Values.of(
        (id = UUID.randomUUID, email = "a@x", age = 20),
        (id = UUID.randomUUID, email = "b@x", age = 30)
      )
    ).compile.af

    // The INSERT's column list comes from the named-tuple field names; the body is the VALUES fragment itself
    // (no wrapping SELECT — we're inserting literal rows).
    assertEquals(
      af.fragment.sql,
      """INSERT INTO "users" ("id", "email", "age") VALUES ($1, $2, $3), ($4, $5, $6)"""
    )
  }

  test("join a Table against a literal VALUES — matches your typical lookup-table pattern") {
    val labels = Values.of(
      (uid = UUID.randomUUID, label = "a"),
      (uid = UUID.randomUUID, label = "b")
    ).alias("labels")

    val af = users
      .innerJoin(labels)
      .on(r => r.users.id ==== r.labels.uid)
      .select(r => (r.users.email, r.labels.label))
      .compile
      .af

    // The INNER JOIN picks up the derived relation's `fromFragmentWith` override — parens + alias + column list.
    assertEquals(
      af.fragment.sql,
      """SELECT "users"."email", "labels"."label" FROM "users" INNER JOIN (VALUES ($1, $2), ($3, $4)) AS "labels" ("uid", "label") ON "users"."id" = "labels"."uid""""
    )
  }

  test("Values.of keeps parameters in declared order across multiple rows") {
    // Three rows × two fields = six $N placeholders in declaration order.
    val af = Values.of(
      (id = 1, label = "a"),
      (id = 2, label = "b"),
      (id = 3, label = "c")
    ).alias("t").select.compile.af

    assertEquals(
      af.fragment.sql,
      """SELECT "id", "label" FROM (VALUES ($1, $2), ($3, $4), ($5, $6)) AS "t" ("id", "label")"""
    )
  }

  test("Option fields in the named tuple become nullable columns (codec `.opt`'d)") {
    // The presence of Option[String] makes the derived column nullable — its codec decodes `Option[String]` rows.
    val af = Values.of(
      (id = 1, nickname = Option("robby")),
      (id = 2, nickname = Option.empty[String])
    ).alias("t").select.compile.af

    assertEquals(
      af.fragment.sql,
      """SELECT "id", "nickname" FROM (VALUES ($1, $2), ($3, $4)) AS "t" ("id", "nickname")"""
    )
  }
}
