/*
 * Copyright 2026 Rui Batista
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    val af = empty.select(_ => (Pg.pi, Pg.random)).compile.af
    assertEquals(af.fragment.sql, "SELECT pi(), random()")
  }

  // ---- NULL handling ----

  test("nullif returns an Option and binds b as a literal parameter") {
    val q                             = products.select(p => Pg.nullif(p.qty, 0)).compile
    val _: CompiledQuery[Option[Int]] = q
    assertEquals(q.af.fragment.sql, """SELECT nullif("qty", $1) FROM "products"""")
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
}
