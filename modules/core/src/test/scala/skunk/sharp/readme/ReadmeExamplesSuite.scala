package skunk.sharp.readme

import cats.effect.IO
import skunk.Session
import skunk.sharp.dsl.*
import skunk.sharp.dsl.given // array codecs (for the unnestRows batch)

import java.time.OffsetDateTime
import java.util.UUID

/**
 * The README's examples, compiled. `README.md` isn't part of the mdoc build, so this keeps its snippets honest — change
 * both together.
 */
object ReadmeExamplesSuite {

  // ---- README: "Describe your tables" ----
  case class User(id: UUID, email: String, age: Int, created_at: OffsetDateTime)
  case class Post(id: UUID, author_id: UUID, title: String, created_at: OffsetDateTime)

  val users = Table.of[User]("users").withPrimary("id").withDefault("id").withUnique("email").withDefault("created_at")
  val posts = Table.of[Post]("posts").withPrimary("id").withDefault("id").withDefault("created_at")

  // ---- README: "Query with typed parameters" ----
  val recentPosts =
    users
      .innerJoin(posts)
      .on(r => r.users.id === r.posts.author_id)
      .where(r => r.users.age >= Param.named["minAge", Int] && r.posts.title.like(Param.named["title", String]))
      .select(r => (email = r.users.email, title = r.posts.title))
      .orderBy(r => r.posts.created_at.desc)
      .limit(20)
      .compile

  def runRecent(session: Session[IO]): IO[List[(email: String, title: String)]] =
    recentPosts.run(session)((minAge = 18, title = "%skunk%"))

  // ---- README: "Upsert with RETURNING" ----
  def upsert(session: Session[IO]): IO[UUID] =
    users
      .insert((email = "ada@example.com", age = 36))
      .onConflict(u => u.email)
      .doUpdateFromExcluded((t, ex) => t.age := ex.age)
      .returning(u => u.id)
      .compile
      .unique(session)

  // ---- README: "Sync a batch with MERGE" ----
  case class Incoming(email: String, age: Int)

  val sync =
    users
      .merge(Pg.unnestRows[Incoming]("rows").alias("incoming"))
      .on(r => r.users.email === r.incoming.email)
      .whenMatched
      .update(r => r.users.age := r.incoming.age)
      .whenNotMatched
      .insert(s => (email = s.email, age = s.age))
      .compile

  def runSync(session: Session[IO]) =
    sync.run(session)((rows = List(Incoming("ada@example.com", 37), Incoming("grace@example.com", 45))))

  // ---- README: "Check the schema at boot" ----
  def checkSchema(session: Session[IO]): IO[Unit] =
    SchemaValidator.validateOrRaise(session, users, posts)

}

class ReadmeExamplesSuite extends munit.FunSuite {
  import ReadmeExamplesSuite.*

  test("the README's query renders as documented") {
    assertEquals(
      recentPosts.fragment.sql,
      """SELECT "users"."email", "posts"."title" FROM "users" INNER JOIN "posts" ON "users"."id" = "posts"."author_id" """ +
        """WHERE ("users"."age" >= $1 AND "posts"."title" LIKE $2) ORDER BY "posts"."created_at" DESC LIMIT 20"""
    )
  }
}
