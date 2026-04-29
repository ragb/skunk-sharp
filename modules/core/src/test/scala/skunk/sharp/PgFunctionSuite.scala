package skunk.sharp

import skunk.sharp.dsl.*

import java.util.UUID

object PgFunctionSuite {
  case class Product(id: UUID, name: String, price: BigDecimal, qty: Int, weight: Double)
}

class PgFunctionSuite extends munit.FunSuite {
  import PgFunctionSuite.*

  private val products = Table.of[Product]("products")

  // ---- Math (same type as input) ----

  test("abs / ceil / floor / trunc / round render correctly and preserve input type") {
    val af = products
      .select(p => (Pg.abs(p.price), Pg.ceil(p.weight), Pg.floor(p.weight), Pg.trunc(p.price), Pg.round(p.price)))
      .compile
      .af
    assertEquals(
      af.fragment.sql,
      """SELECT abs("price"), ceil("weight"), floor("weight"), trunc("price"), round("price") FROM "products""""
    )
  }

  test("round with digits") {
    val af = products.select(p => Pg.round(p.price, 2)).compile.af
    assertEquals(af.fragment.sql, """SELECT round("price", 2) FROM "products"""")
  }

  test("mod(a, b)") {
    val af = products.select(p => Pg.mod(p.qty, p.qty)).compile.af
    assertEquals(af.fragment.sql, """SELECT mod("qty", "qty") FROM "products"""")
  }

  test("greatest / least — varargs, same type across args") {
    val af = products
      .select(p => (Pg.greatest(p.price, p.price), Pg.least(p.weight, p.weight, p.weight)))
      .compile
      .af
    assertEquals(
      af.fragment.sql,
      """SELECT greatest("price", "price"), least("weight", "weight", "weight") FROM "products""""
    )
  }

  // ---- Math returning Double via Lift ----

  test("sqrt / ln / exp / log / power return Double via Lift") {
    val af = products
      .select(p => (Pg.sqrt(p.price), Pg.ln(p.weight), Pg.exp(p.weight), Pg.log(p.price), Pg.power(p.qty, p.qty)))
      .compile
      .af
    assertEquals(
      af.fragment.sql,
      """SELECT sqrt("price"), ln("weight"), exp("weight"), log("price"), power("qty", "qty") FROM "products""""
    )
  }

  test("pi / random") {
    val af = empty.select((Pg.pi, Pg.random)).compile.af
    assertEquals(af.fragment.sql, "SELECT pi(), random()")
  }

  // ---- NULL handling ----

  test("nullif returns an Option and binds b as a literal parameter") {
    val q                             = products.select(p => Pg.nullif(p.qty, 0)).compile
    val _: QueryTemplate[?, Option[Int]] = q
    assertEquals(q.fragment.sql, """SELECT nullif("qty", $1) FROM "products"""")
  }

  // ---- String (preserve tag) ----

  test("trim / ltrim / rtrim") {
    val af = products
      .select(p => (Pg.trim(p.name), Pg.ltrim(p.name), Pg.rtrim(p.name)))
      .compile
      .af
    assertEquals(
      af.fragment.sql,
      """SELECT trim("name"), ltrim("name"), rtrim("name") FROM "products""""
    )
  }

  test("trim with chars arg — `trim(chars FROM s)`") {
    val af = products.select(p => Pg.trim(p.name, " \t")).compile.af
    assertEquals(af.fragment.sql, """SELECT trim($1 FROM "name") FROM "products"""")
  }

  test("replace / repeat / reverse") {
    val af = products
      .select(p => (Pg.replace(p.name, "a", "b"), Pg.repeat(p.name, 3), Pg.reverse(p.name)))
      .compile
      .af
    assert(af.fragment.sql.contains("""replace("name", $1, $2)"""), af.fragment.sql)
    assert(af.fragment.sql.contains("""repeat("name", 3)"""), af.fragment.sql)
    assert(af.fragment.sql.contains("""reverse("name")"""), af.fragment.sql)
  }

  test("substring — single-arg and with FOR length") {
    val af = products
      .select(p => (Pg.substring(p.name, 2), Pg.substring(p.name, 2, 3)))
      .compile
      .af
    assertEquals(
      af.fragment.sql,
      """SELECT substring("name" FROM 2), substring("name" FROM 2 FOR 3) FROM "products""""
    )
  }

  test("left / right") {
    val af = products.select(p => (Pg.left(p.name, 5), Pg.right(p.name, 5))).compile.af
    assertEquals(
      af.fragment.sql,
      """SELECT left("name", 5), right("name", 5) FROM "products""""
    )
  }

  test("regexpReplace / splitPart") {
    val af = products
      .select(p => (Pg.regexpReplace(p.name, "^[a-z]+", ""), Pg.splitPart(p.name, "-", 1)))
      .compile
      .af
    assert(af.fragment.sql.contains("""regexp_replace("name", $1, $2)"""), af.fragment.sql)
    assert(af.fragment.sql.contains("""split_part("name", $3, 1)"""), af.fragment.sql)
  }

  // ---- String returning Int via Lift ----

  test("charLength / octetLength / position — Lift preserves input nullability") {
    val af = products
      .select(p => (Pg.charLength(p.name), Pg.octetLength(p.name), Pg.position("x", p.name)))
      .compile
      .af
    assertEquals(
      af.fragment.sql,
      """SELECT char_length("name"), octet_length("name"), position($1 IN "name") FROM "products""""
    )
  }

  // ---- Tier 2: sign / trig / hyperbolic / degrees/radians ----

  test("sign preserves input type") {
    val af = products.select(p => (Pg.sign(p.price), Pg.sign(p.qty))).compile.af
    assertEquals(af.fragment.sql, """SELECT sign("price"), sign("qty") FROM "products"""")
  }

  test("degrees / radians return Double via Lift") {
    val af = products.select(p => (Pg.degrees(p.weight), Pg.radians(p.weight))).compile.af
    assertEquals(af.fragment.sql, """SELECT degrees("weight"), radians("weight") FROM "products"""")
  }

  test("sin / cos / tan / asin / acos / atan return Double via Lift") {
    val af = products
      .select(p =>
        (
          Pg.sin(p.weight),
          Pg.cos(p.weight),
          Pg.tan(p.weight),
          Pg.asin(p.weight),
          Pg.acos(p.weight),
          Pg.atan(p.weight)
        )
      )
      .compile.af
    assertEquals(
      af.fragment.sql,
      """SELECT sin("weight"), cos("weight"), tan("weight"), asin("weight"), acos("weight"), atan("weight") FROM "products""""
    )
  }

  test("atan2(y, x) renders two-argument form") {
    val af = products.select(p => Pg.atan2(p.weight, p.weight)).compile.af
    assertEquals(af.fragment.sql, """SELECT atan2("weight", "weight") FROM "products"""")
  }

  test("sinh / cosh / tanh return Double via Lift") {
    val af = products.select(p => (Pg.sinh(p.weight), Pg.cosh(p.weight), Pg.tanh(p.weight))).compile.af
    assertEquals(
      af.fragment.sql,
      """SELECT sinh("weight"), cosh("weight"), tanh("weight") FROM "products""""
    )
  }

  // ---- Tier 2: string functions ----

  test("initcap / translate / lpad / rpad preserve tag") {
    val af = products
      .select(p =>
        (
          Pg.initcap(p.name),
          Pg.translate(p.name, "abc", "xyz"),
          Pg.lpad(p.name, 10),
          Pg.rpad(p.name, 10, "*")
        )
      )
      .compile.af
    assert(af.fragment.sql.contains("""initcap("name")"""), af.fragment.sql)
    assert(af.fragment.sql.contains("""translate("name", $"""), af.fragment.sql)
    assert(af.fragment.sql.contains("""lpad("name", 10)"""), af.fragment.sql)
    assert(af.fragment.sql.contains("""rpad("name", 10, $"""), af.fragment.sql)
  }

  test("md5 returns string via Lift") {
    val af = products.select(p => Pg.md5(p.name)).compile.af
    assertEquals(af.fragment.sql, """SELECT md5("name") FROM "products"""")
  }

  test("ascii returns Int via Lift") {
    val af = products.select(p => Pg.ascii(p.name)).compile.af
    assertEquals(af.fragment.sql, """SELECT ascii("name") FROM "products"""")
  }

  test("chr takes Int-like TypedExpr, returns String via Lift") {
    val af = products.select(p => Pg.chr(p.qty)).compile.af
    assertEquals(af.fragment.sql, """SELECT chr("qty") FROM "products"""")
  }

  test("toChar renders to_char(e, $fmt)") {
    val af = products.select(p => Pg.toChar(p.price, "FM999990.00")).compile.af
    assert(af.fragment.sql.startsWith("""SELECT to_char("price", $"""), af.fragment.sql)
  }

  test("toNumber renders to_number(e, $fmt)") {
    val af = products.select(p => Pg.toNumber(p.name, "99.99")).compile.af
    assert(af.fragment.sql.contains("""to_number("name", $"""), af.fragment.sql)
  }

  test("format renders format($fmt, args…)") {
    val af = products.select(p => Pg.format("%s-%s", p.name, p.name)).compile.af
    assert(af.fragment.sql.contains("""format($"""), af.fragment.sql)
    assert(af.fragment.sql.contains(""""name""""), af.fragment.sql)
  }

  // ---- Tier 2: date / time ----

  test("extract renders extract(field FROM e)") {
    val af = empty.select(_ => Pg.extract("year", Pg.now)).compile.af
    assertEquals(af.fragment.sql, "SELECT extract(year FROM now())")
  }

  test("dateTrunc renders date_trunc($precision, e)") {
    val af = empty.select(_ => Pg.dateTrunc("month", Pg.now)).compile.af
    assert(af.fragment.sql.contains("date_trunc($"), af.fragment.sql)
    assert(af.fragment.sql.contains("now()"), af.fragment.sql)
  }

  test("age(a, b) renders age(a, b)") {
    val af = empty.select(_ => Pg.age(Pg.now, Pg.now)).compile.af
    assertEquals(af.fragment.sql, "SELECT age(now(), now())")
  }

  test("makeDate / makeTime / makeTimestamp render correctly") {
    val af = empty.select(_ => Pg.makeDate(lit(2024), lit(1), lit(15))).compile.af
    assert(af.fragment.sql.contains("make_date("), af.fragment.sql)
  }

  test("toTimestamp renders to_timestamp(e)") {
    val af = empty.select(_ => Pg.toTimestamp(param(0.0))).compile.af
    assertEquals(af.fragment.sql, "SELECT to_timestamp($1)")
  }

  test("toDate renders to_date(e, $fmt)") {
    val af = empty.select(_ => Pg.toDate(param("2024-01-15"), "YYYY-MM-DD")).compile.af
    assert(af.fragment.sql.contains("to_date("), af.fragment.sql)
  }

  // ---- Tier 2: stat aggregates ----

  test("stddev / variance / corr / regrCount render correctly") {
    val af = products
      .select(p =>
        (
          Pg.stddev(p.weight),
          Pg.variance(p.weight),
          Pg.corr(p.weight, p.weight),
          Pg.regrCount(p.weight, p.weight)
        )
      )
      .compile.af
    assert(af.fragment.sql.contains("""stddev("weight")"""), af.fragment.sql)
    assert(af.fragment.sql.contains("""variance("weight")"""), af.fragment.sql)
    assert(af.fragment.sql.contains("""corr("weight", "weight")"""), af.fragment.sql)
    assert(af.fragment.sql.contains("""regr_count("weight", "weight")"""), af.fragment.sql)
  }

  test("stddevPop / stddevSamp / varPop / varSamp / covarPop / covarSamp render correctly") {
    val af = products
      .select(p =>
        (
          Pg.stddevPop(p.weight),
          Pg.stddevSamp(p.weight),
          Pg.varPop(p.weight),
          Pg.varSamp(p.weight),
          Pg.covarPop(p.weight, p.weight),
          Pg.covarSamp(p.weight, p.weight)
        )
      )
      .compile.af
    assert(af.fragment.sql.contains("stddev_pop("), af.fragment.sql)
    assert(af.fragment.sql.contains("stddev_samp("), af.fragment.sql)
    assert(af.fragment.sql.contains("var_pop("), af.fragment.sql)
    assert(af.fragment.sql.contains("var_samp("), af.fragment.sql)
    assert(af.fragment.sql.contains("covar_pop("), af.fragment.sql)
    assert(af.fragment.sql.contains("covar_samp("), af.fragment.sql)
  }

  test("regr_* functions render correctly") {
    val af = products
      .select(p =>
        (
          Pg.regrSlope(p.weight, p.weight),
          Pg.regrIntercept(p.weight, p.weight),
          Pg.regrR2(p.weight, p.weight),
          Pg.regrAvgX(p.weight, p.weight),
          Pg.regrAvgY(p.weight, p.weight),
          Pg.regrSxx(p.weight, p.weight),
          Pg.regrSyy(p.weight, p.weight),
          Pg.regrSxy(p.weight, p.weight)
        )
      )
      .compile.af
    assert(af.fragment.sql.contains("regr_slope("), af.fragment.sql)
    assert(af.fragment.sql.contains("regr_intercept("), af.fragment.sql)
    assert(af.fragment.sql.contains("regr_r2("), af.fragment.sql)
    assert(af.fragment.sql.contains("regr_avgx("), af.fragment.sql)
    assert(af.fragment.sql.contains("regr_sxx("), af.fragment.sql)
  }
}
