package skunk.sharp

import skunk.sharp.dsl.*
import skunk.sharp.dsl.given
import skunk.sharp.pg.ArrayOps.*
import skunk.data.Arr
import java.util.UUID

object ReproDiag {
  case class Post(id: UUID, title: String, tags: Arr[String])
  val posts = Table.of[Post]("posts")
}

class ReproDiag extends munit.FunSuite {
  import ReproDiag.*

  test("diag: array contains") {
    val q = posts.select.where(p => p.tags.contains(param(Arr("scala", "pg")))).compile
    println(s"SQL:           ${q.fragment.sql}")
    println(s"Encoder class: ${q.fragment.encoder.getClass.getName}")
    println(s"Encoder types: ${q.fragment.encoder.types}")
    val encoded = q.fragment.encoder.encode(skunk.Void)
    println(s"Encoded: $encoded")
  }

  test("diag: between value-overload") {
    case class U(id: java.util.UUID, age: Int)
    val users = Table.of[U]("users")
    val q = users.select.where(u => u.age.between(18, 25)).compile
    println(s"between SQL:   ${q.fragment.sql}")
    println(s"between class: ${q.fragment.encoder.getClass.getName}")
    val encoded = q.fragment.encoder.encode(skunk.Void)
    println(s"between encoded: $encoded")
  }

  test("diag: jsonb-like getText === value pattern") {
    case class U(id: java.util.UUID, name: String)
    val users = Table.of[U]("users")
    // Simulates: column-derived TypedExpr piped through value-overload ===
    val q = users.select.where(u => u.name === "alice").compile
    println(s"jsonb-ish SQL: ${q.fragment.sql}")
    val encoded = q.fragment.encoder.encode(skunk.Void)
    println(s"jsonb-ish encoded: $encoded")
  }
}
