package skunk.sharp.tests

import skunk.sharp.dsl.*

/**
 * Integration tests for the built-in [[skunk.sharp.Pg]] functions. Uses `empty.select(...)` everywhere so we don't need
 * a table — every test is a FROM-less SELECT that exercises one function end-to-end.
 */
class PgFunctionSuite extends PgFixture {

  // ---- Math (same type as input) ----

  test("abs / ceil / floor / trunc / round round-trip against Postgres") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- assertIO(empty.select(_ => Pg.abs(lit(-42))).compile.unique(s), 42)
          _ <- assertIO(empty.select(_ => Pg.ceil(lit(BigDecimal("1.2")))).compile.unique(s), BigDecimal(2))
          _ <- assertIO(empty.select(_ => Pg.floor(lit(BigDecimal("1.8")))).compile.unique(s), BigDecimal(1))
          _ <- assertIO(empty.select(_ => Pg.trunc(lit(BigDecimal("-1.9")))).compile.unique(s), BigDecimal(-1))
          _ <- assertIO(empty.select(_ => Pg.round(lit(BigDecimal("1.5")))).compile.unique(s), BigDecimal(2))
        } yield ()
      }
    }
  }

  test("round(x, digits)") {
    withContainers { containers =>
      session(containers).use { s =>
        assertIO(
          empty.select(_ => Pg.round(lit(BigDecimal("1.2345")), 2)).compile.unique(s),
          BigDecimal("1.23")
        )
      }
    }
  }

  test("mod(a, b)") {
    withContainers { containers =>
      session(containers).use { s =>
        assertIO(empty.select(_ => Pg.mod(lit(10), lit(3))).compile.unique(s), 1)
      }
    }
  }

  test("greatest / least") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- assertIO(empty.select(_ => Pg.greatest(lit(1), lit(5), lit(3))).compile.unique(s), 5)
          _ <- assertIO(empty.select(_ => Pg.least(lit(1), lit(5), lit(3))).compile.unique(s), 1)
        } yield ()
      }
    }
  }

  // ---- Math returning Double ----

  test("sqrt / power / exp / ln / log") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- assertIO(empty.select(_ => Pg.sqrt(lit(9.0))).compile.unique(s), 3.0)
          _ <- assertIO(empty.select(_ => Pg.power(lit(2.0), lit(10.0))).compile.unique(s), 1024.0)
          _ <- assertIO(empty.select(_ => Pg.exp(lit(0.0))).compile.unique(s), 1.0)
          _ <- assertIO(empty.select(_ => Pg.ln(lit(1.0))).compile.unique(s), 0.0)
          _ <- assertIO(empty.select(_ => Pg.log(lit(100.0))).compile.unique(s), 2.0)
        } yield ()
      }
    }
  }

  test("pi / random (random bounded in [0, 1))") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          p <- empty.select(_ => Pg.pi).compile.unique(s)
          r <- empty.select(_ => Pg.random).compile.unique(s)
          _ = assert(math.abs(p - math.Pi) < 1e-9, s"pi was $p")
          _ = assert(r >= 0.0 && r < 1.0, s"random out of range: $r")
        } yield ()
      }
    }
  }

  // ---- NULL handling ----

  test("nullif — equal → NULL, unequal → the first value") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- assertIO(empty.select(_ => Pg.nullif(lit(5), 5)).compile.unique(s), Option.empty[Int])
          _ <- assertIO(empty.select(_ => Pg.nullif(lit(5), 3)).compile.unique(s), Option(5))
        } yield ()
      }
    }
  }

  // ---- String functions preserving tag (return String) ----

  test("lower / upper") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- assertIO(empty.select(_ => Pg.lower(lit("HELLO"))).compile.unique(s), "hello")
          _ <- assertIO(empty.select(_ => Pg.upper(lit("hello"))).compile.unique(s), "HELLO")
        } yield ()
      }
    }
  }

  test("trim / ltrim / rtrim / trim(chars, s)") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- assertIO(empty.select(_ => Pg.trim(lit("  hi  "))).compile.unique(s), "hi")
          _ <- assertIO(empty.select(_ => Pg.ltrim(lit("  hi  "))).compile.unique(s), "hi  ")
          _ <- assertIO(empty.select(_ => Pg.rtrim(lit("  hi  "))).compile.unique(s), "  hi")
          _ <- assertIO(empty.select(_ => Pg.trim(lit("xxhiyy"), "xy")).compile.unique(s), "hi")
        } yield ()
      }
    }
  }

  test("replace / substring / left / right / repeat / reverse") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- assertIO(empty.select(_ => Pg.replace(lit("a-b-c"), "-", "/")).compile.unique(s), "a/b/c")
          _ <- assertIO(empty.select(_ => Pg.substring(lit("hello world"), 7)).compile.unique(s), "world")
          _ <- assertIO(empty.select(_ => Pg.substring(lit("hello world"), 1, 5)).compile.unique(s), "hello")
          _ <- assertIO(empty.select(_ => Pg.left(lit("hello"), 3)).compile.unique(s), "hel")
          _ <- assertIO(empty.select(_ => Pg.right(lit("hello"), 3)).compile.unique(s), "llo")
          _ <- assertIO(empty.select(_ => Pg.repeat(lit("ab"), 3)).compile.unique(s), "ababab")
          _ <- assertIO(empty.select(_ => Pg.reverse(lit("abc"))).compile.unique(s), "cba")
        } yield ()
      }
    }
  }

  test("regexpReplace / splitPart") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <-
            assertIO(empty.select(_ => Pg.regexpReplace(lit("foo123bar"), "[0-9]+", "-")).compile.unique(s), "foo-bar")
          _ <- assertIO(empty.select(_ => Pg.splitPart(lit("a-b-c"), "-", 2)).compile.unique(s), "b")
        } yield ()
      }
    }
  }

  // ---- String functions returning Int ----

  test("length / charLength / octetLength / position") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- assertIO(empty.select(_ => Pg.length(lit("abc"))).compile.unique(s), 3)
          _ <- assertIO(empty.select(_ => Pg.charLength(lit("abc"))).compile.unique(s), 3)
          _ <- assertIO(empty.select(_ => Pg.octetLength(lit("abc"))).compile.unique(s), 3)
          _ <- assertIO(empty.select(_ => Pg.position("b", lit("abc"))).compile.unique(s), 2)
        } yield ()
      }
    }
  }

  // ---- Time accessors ----

  test("now / currentTimestamp / currentDate / localTimestamp — all return the expected SQL types") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          n  <- empty.select(_ => Pg.now).compile.unique(s)
          ct <- empty.select(_ => Pg.currentTimestamp).compile.unique(s)
          cd <- empty.select(_ => Pg.currentDate).compile.unique(s)
          lt <- empty.select(_ => Pg.localTimestamp).compile.unique(s)
          // Not asserting absolute values — just that we decoded something non-null.
          _ = assert(n.toEpochSecond > 0L, n.toString)
          _ = assert(ct.toEpochSecond > 0L, ct.toString)
          _ = assert(cd.toEpochDay > 0L, cd.toString)
          _ = assert(lt.toLocalDate.toEpochDay > 0L, lt.toString)
        } yield ()
      }
    }
  }

  test("concat / coalesce") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- assertIO(empty.select(_ => Pg.concat(lit("a"), lit("b"), lit("c"))).compile.unique(s), "abc")
          _ <- assertIO(
            empty.select(_ =>
              Pg.coalesce(lit[Option[String]](None), lit[Option[String]](Some("fallback")))
            ).compile.unique(s),
            Option("fallback")
          )
        } yield ()
      }
    }
  }
}
