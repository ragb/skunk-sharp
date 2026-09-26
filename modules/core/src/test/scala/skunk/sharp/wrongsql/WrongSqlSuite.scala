package skunk.sharp.wrongsql

import skunk.AppliedFragment
import skunk.sharp.dsl.*

import java.time.OffsetDateTime
import java.util.UUID
import scala.compiletime.testing.typeCheckErrors

object WrongSqlSuite {
  case class User(id: UUID, email: String, age: Int)
  case class Post(id: UUID, user_id: UUID, title: String, created_at: OffsetDateTime)
  case class Tag(id: UUID, post_id: UUID, name: String)
  val users = Table.of[User]("users")
  val posts = Table.of[Post]("posts")
  val tags  = Table.of[Tag]("tags")
}

/** Regression tests for builder chains that compiled but rendered wrong SQL (audit batch 2). */
class WrongSqlSuite extends munit.FunSuite {
  import WrongSqlSuite.*

  private def encoded(af: AppliedFragment): List[Option[String]] =
    af.fragment.encoder.encode(af.argument).map(_.map(_.value))

  test("joining from a builder keeps its WHERE / ORDER BY / LIMIT / OFFSET / DISTINCT") {
    val q = users.select
      .where(u => u.age >= Param[Int])
      .orderBy(u => u.email.asc)
      .limit(5)
      .offset(10)
      .distinctRows
      .innerJoin(posts)
      .on(r => r.users.id === r.posts.user_id)
      .select(r => (r.users.email, r.posts.title))
      .compile
    val _: QueryTemplate[Int, ?] = q
    val sql                      = q.fragment.sql
    assert(sql.startsWith("""SELECT DISTINCT "users"."email", "posts"."title" FROM"""), sql)
    assert(
      sql.endsWith(
        """FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."user_id" WHERE "age" >= $1 ORDER BY "email" ASC LIMIT 5 OFFSET 10"""
      ),
      sql
    )
  }

  test("ON Params bind before carried WHERE Params (render order)") {
    val q = users.select
      .where(u => u.age >= Param[Int])
      .innerJoin(posts)
      .on(r => r.users.id === r.posts.user_id && r.posts.title === Param[String])
      .select(r => r.posts.title)
      .compile
    val _: QueryTemplate[(String, Int), ?] = q
    assert(q.fragment.sql.contains(""""posts"."title" = $1) WHERE "age" >= $2"""), q.fragment.sql)
    assertEquals(encoded(q.bind(("t", 3))), List(Some("t"), Some("3")))
  }

  test("a WHERE between two joins survives the second join") {
    val q = users
      .innerJoin(posts)
      .on(r => r.users.id === r.posts.user_id)
      .where(r => r.users.age >= Param[Int])
      .leftJoin(tags)
      .on(r => r.tags.post_id === r.posts.id)
      .select(r => r.tags.name)
      .compile
    val _: QueryTemplate[Int, ?] = q
    assert(q.fragment.sql.endsWith("""ON "tags"."post_id" = "posts"."id" WHERE "users"."age" >= $1"""), q.fragment.sql)
  }

  test("crossJoin from a builder keeps DISTINCT ON") {
    val q = users.select.distinctOn(u => u.email).crossJoin(tags).select(r => r.tags.name).compile
    assert(q.fragment.sql.startsWith("""SELECT DISTINCT ON ("email") """), q.fragment.sql)
  }

  test("LATERAL started from a builder qualifies the outer columns") {
    val q = users.select
      .innerJoinLateral(u => posts.select.where(p => p.user_id === u.id).limit(3).alias("recent"))
      .on(_ => lit(true))
      .select(r => r.recent.title)
      .compile
    assert(q.fragment.sql.contains("""WHERE "user_id" = "users"."id" LIMIT 3"""), q.fragment.sql)
  }

  test("a RIGHT JOIN after a LATERAL source keeps the LATERAL keyword") {
    val q = users
      .crossJoinLateral(u => posts.select.where(p => p.user_id === u.id).limit(1).alias("top"))
      .rightJoin(tags)
      .on(r => r.tags.post_id === r.top.id)
      .select(r => r.tags.name)
      .compile
    assert(q.fragment.sql.contains("CROSS JOIN LATERAL ("), q.fragment.sql)
  }

  test("UPDATE … FROM: assigning to a FROM source's column is a compile error") {
    val errs = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import WrongSqlSuite.*
      users.update.from(posts).set(r => r.posts.title := r.users.email)
    """)
    assert(errs.nonEmpty)
    // Assigning the target from a source still compiles.
    val ok =
      users.update.from(posts).set(r => r.users.email := r.posts.title).where(r => r.users.id === r.posts.user_id)
    assert(ok.compile.af.fragment.sql.startsWith("""UPDATE "users" SET "email" = "posts"."title" FROM "posts""""))
  }

  test("insert.withParams: a Param of the wrong type for its column is a compile error") {
    val errs = typeCheckErrors("""
      import skunk.sharp.dsl.*
      import java.util.UUID
      import WrongSqlSuite.*
      users.insert.withParams((id = Param[UUID], email = Param[String], age = Param[String]))
    """)
    assert(errs.nonEmpty)
    val named = users.insert
      .withParams((id = Param.named["id", UUID], email = Param.named["email", String], age = Param.named["age", Int]))
      .compile
    assert(named.fragment.sql.startsWith("""INSERT INTO "users""""), named.fragment.sql)
  }
}
