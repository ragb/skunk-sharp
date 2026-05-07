package skunk.sharp

import skunk.Void
import skunk.sharp.dsl.*

import java.util.UUID

object ExprInterpolatorSuite {
  case class User(id: UUID, email: String, age: Int)
}

class ExprInterpolatorSuite extends munit.FunSuite {
  import ExprInterpolatorSuite.User

  private val users = Table.of[User]("users")

  // -------- No-arg form ------------------------------------------------------------------------

  test("expr with no interpolations renders the literal SQL and Args = Void") {
    val e                                            = expr"now()".as[java.time.OffsetDateTime]
    val _: TypedExpr[java.time.OffsetDateTime, Void] = e
    assertEquals(e.fragment.sql, "now()")
  }

  // -------- TypedExpr (column) interpolation ---------------------------------------------------

  test("expr splicing a single column ref preserves Args = Void") {
    val q = users.select(u => expr"lower(${u.email})".asCodec(skunk.codec.all.text)).compile
    val _: QueryTemplate[Void, String] = q
    assert(q.fragment.sql.contains("""lower("email")"""), q.fragment.sql)
  }

  test("expr splicing two column refs renders with literal separator + Args = Void") {
    val q = users.select(u => expr"${u.email} || ${u.email}".asCodec(skunk.codec.all.text)).compile
    val _: QueryTemplate[Void, String] = q
    assert(q.fragment.sql.contains(""""email" || "email""""), q.fragment.sql)
  }

  // -------- Param interpolation (TypedExpr branch) ---------------------------------------------

  test("expr splicing a Param[T] threads Args = T") {
    val q = users.select(u => expr"lower(${u.email}) = lower(${Param[String]})".as[Boolean]).compile
    val _: QueryTemplate[String, Boolean] = q
    assert(q.fragment.sql.contains("$1"), q.fragment.sql)
  }

  test("expr splicing two Params via FoldConcat collapses to flat tuple Args") {
    val q                                 = users.select(_ => expr"${Param[Int]} + ${Param[Int]}".as[Int]).compile
    val _: QueryTemplate[(Int, Int), Int] = q
    assert(q.fragment.sql.contains("$1 + $2"), q.fragment.sql)
  }

  test("expr splicing column + Param threads Args = the Param's type") {
    val q                          = users.select(u => expr"${u.age} + ${Param[Int]}".as[Int]).compile
    val _: QueryTemplate[Int, Int] = q
    assert(q.fragment.sql.contains(""""age" + $1"""), q.fragment.sql)
  }

  // -------- Value interpolation requires explicit wrapping ------------------------------------

  test("expr with lit(v) for a compile-time literal — Args = Void") {
    val q = users.select(u => expr"${u.email} = ${lit("@example.com")}".as[Boolean]).compile
    val _: QueryTemplate[Void, Boolean] = q
    assert(q.fragment.sql.contains("@example.com"), q.fragment.sql)
  }

  test("expr with Param.bind(v) for a runtime value — Args = Void") {
    val n                               = 18
    val q                               = users.select(u => expr"${u.age} >= ${Param.bind(n)}".as[Boolean]).compile
    val _: QueryTemplate[Void, Boolean] = q
    assert(q.fragment.sql.contains("$1"), q.fragment.sql)
  }

  // -------- Mixed shapes ------------------------------------------------------------------------

  test("expr mixing TypedExpr Params and baked values composes Args = only the Params") {
    val q = users.select(u => expr"${u.age} >= ${Param[Int]} AND ${u.age} <= ${lit(99)}".as[Boolean]).compile
    val _: QueryTemplate[Int, Boolean] = q
  }

  // -------- Encoder execution path -------------------------------------------------------------

  test("encoder for a Param-bearing expr binds the supplied value at execute") {
    val q       = users.select(u => expr"${u.age} + ${Param[Int]}".as[Int]).compile
    val af      = q.bind(7)
    val encoded = af.fragment.encoder.encode(af.argument).flatten.map(_.value)
    assertEquals(encoded, List("7"))
  }

  test("encoder for two-Param expr binds both supplied values in render order") {
    val q       = users.select(_ => expr"${Param[Int]} + ${Param[Int]}".as[Int]).compile
    val af      = q.bind((3, 4))
    val encoded = af.fragment.encoder.encode(af.argument).flatten.map(_.value)
    assertEquals(encoded, List("3", "4"))
  }

  test("encoder for a Param + Param.bind expr emits both — execute-time + baked") {
    val n       = 42
    val q       = users.select(_ => expr"${Param[Int]} + ${Param.bind(n)}".as[Int]).compile
    val af      = q.bind(8)
    val encoded = af.fragment.encoder.encode(af.argument).flatten.map(_.value)
    assertEquals(encoded, List("8", "42"))
  }

  // -------- Negative test: bare values rejected at compile time --------------------------------

  test("expr with bare runtime value does not compile — message points to lit / Param / Param.bind") {
    val errors = scala.compiletime.testing.typeCheckErrors(
      "import skunk.sharp.*; import skunk.sharp.dsl.*; val n = 18; val e = expr\"$n + ${lit(1)}\".as[Int]"
    )
    assert(
      errors.exists(_.message.contains("Wrap it explicitly")),
      s"expected the macro's wrap-it-explicitly diagnostic, got: ${errors.map(_.message).mkString("; ")}"
    )
  }

  // -------- .as / .asCodec parity --------------------------------------------------------------

  test("expr.asCodec(codec) reuses an explicit codec") {
    val q = users.select(u => expr"upper(${u.email})".asCodec(u_emailCodec)).compile
    assertEquals(q.fragment.sql.trim, """SELECT upper("email") FROM "users"""")
  }
  private val u_emailCodec: skunk.Codec[String] = skunk.codec.all.text

  test("expr.asCodec(e) reuses the spliced expression's codec without naming the codec") {
    val q                              = users.select(u => expr"upper(${u.email})".asCodec(u.email)).compile
    val _: QueryTemplate[Void, String] = q
    assertEquals(q.fragment.sql.trim, """SELECT upper("email") FROM "users"""")
  }

  // -------- Used inside a WHERE clause via Where.fromTypedExpr ---------------------------------

  test("expr-built boolean expression slots into WHERE through Where.fromTypedExpr") {
    val q = users.select
      .where(u => skunk.sharp.where.Where.fromTypedExpr(expr"${u.age} >= ${Param[Int]}".as[Boolean]))
      .compile
    val _: QueryTemplate[Int, ?] = q
    assert(q.fragment.sql.contains("$1"), q.fragment.sql)
  }
}
