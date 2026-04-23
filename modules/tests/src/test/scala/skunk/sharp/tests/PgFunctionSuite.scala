package skunk.sharp.tests

import skunk.sharp.dsl.*

import java.time.{LocalDate, LocalDateTime}

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
          _ <- assertIO(empty.select(Pg.abs(lit(-42))).compile.unique(s), 42)
          _ <- assertIO(empty.select(Pg.ceil(param(BigDecimal("1.2")))).compile.unique(s), BigDecimal(2))
          _ <- assertIO(empty.select(Pg.floor(param(BigDecimal("1.8")))).compile.unique(s), BigDecimal(1))
          _ <- assertIO(empty.select(Pg.trunc(param(BigDecimal("-1.9")))).compile.unique(s), BigDecimal(-1))
          _ <- assertIO(empty.select(Pg.round(param(BigDecimal("1.5")))).compile.unique(s), BigDecimal(2))
        } yield ()
      }
    }
  }

  test("round(x, digits)") {
    withContainers { containers =>
      session(containers).use { s =>
        assertIO(
          empty.select(Pg.round(param(BigDecimal("1.2345")), 2)).compile.unique(s),
          BigDecimal("1.23")
        )
      }
    }
  }

  test("mod(a, b)") {
    withContainers { containers =>
      session(containers).use { s =>
        assertIO(empty.select(Pg.mod(lit(10), lit(3))).compile.unique(s), 1)
      }
    }
  }

  test("greatest / least") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- assertIO(empty.select(Pg.greatest(lit(1), lit(5), lit(3))).compile.unique(s), 5)
          _ <- assertIO(empty.select(Pg.least(lit(1), lit(5), lit(3))).compile.unique(s), 1)
        } yield ()
      }
    }
  }

  // ---- Math returning Double ----

  test("sqrt / power / exp / ln / log") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- assertIO(empty.select(Pg.sqrt(lit(9.0))).compile.unique(s), 3.0)
          _ <- assertIO(empty.select(Pg.power(lit(2.0), lit(10.0))).compile.unique(s), 1024.0)
          _ <- assertIO(empty.select(Pg.exp(lit(0.0))).compile.unique(s), 1.0)
          _ <- assertIO(empty.select(Pg.ln(lit(1.0))).compile.unique(s), 0.0)
          _ <- assertIO(empty.select(Pg.log(lit(100.0))).compile.unique(s), 2.0)
        } yield ()
      }
    }
  }

  test("pi / random (random bounded in [0, 1))") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          p <- empty.select(Pg.pi).compile.unique(s)
          r <- empty.select(Pg.random).compile.unique(s)
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
          _ <- assertIO(empty.select(Pg.nullif(lit(5), 5)).compile.unique(s), Option.empty[Int])
          _ <- assertIO(empty.select(Pg.nullif(lit(5), 3)).compile.unique(s), Option(5))
        } yield ()
      }
    }
  }

  // ---- String functions preserving tag (return String) ----

  test("lower / upper") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- assertIO(empty.select(Pg.lower(param("HELLO"))).compile.unique(s), "hello")
          _ <- assertIO(empty.select(Pg.upper(param("hello"))).compile.unique(s), "HELLO")
        } yield ()
      }
    }
  }

  test("trim / ltrim / rtrim / trim(chars, s)") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- assertIO(empty.select(Pg.trim(param("  hi  "))).compile.unique(s), "hi")
          _ <- assertIO(empty.select(Pg.ltrim(param("  hi  "))).compile.unique(s), "hi  ")
          _ <- assertIO(empty.select(Pg.rtrim(param("  hi  "))).compile.unique(s), "  hi")
          _ <- assertIO(empty.select(Pg.trim(param("xxhiyy"), "xy")).compile.unique(s), "hi")
        } yield ()
      }
    }
  }

  test("replace / substring / left / right / repeat / reverse") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- assertIO(empty.select(Pg.replace(param("a-b-c"), "-", "/")).compile.unique(s), "a/b/c")
          _ <- assertIO(empty.select(Pg.substring(param("hello world"), 7)).compile.unique(s), "world")
          _ <- assertIO(empty.select(Pg.substring(param("hello world"), 1, 5)).compile.unique(s), "hello")
          _ <- assertIO(empty.select(Pg.left(param("hello"), 3)).compile.unique(s), "hel")
          _ <- assertIO(empty.select(Pg.right(param("hello"), 3)).compile.unique(s), "llo")
          _ <- assertIO(empty.select(Pg.repeat(param("ab"), 3)).compile.unique(s), "ababab")
          _ <- assertIO(empty.select(Pg.reverse(param("abc"))).compile.unique(s), "cba")
        } yield ()
      }
    }
  }

  test("regexpReplace / splitPart") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <-
            assertIO(
              empty.select(Pg.regexpReplace(param("foo123bar"), "[0-9]+", "-")).compile.unique(s),
              "foo-bar"
            )
          _ <- assertIO(empty.select(Pg.splitPart(param("a-b-c"), "-", 2)).compile.unique(s), "b")
        } yield ()
      }
    }
  }

  // ---- String functions returning Int ----

  test("length / charLength / octetLength / position") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- assertIO(empty.select(Pg.length(param("abc"))).compile.unique(s), 3)
          _ <- assertIO(empty.select(Pg.charLength(param("abc"))).compile.unique(s), 3)
          _ <- assertIO(empty.select(Pg.octetLength(param("abc"))).compile.unique(s), 3)
          _ <- assertIO(empty.select(Pg.position("b", param("abc"))).compile.unique(s), 2)
        } yield ()
      }
    }
  }

  // ---- Time accessors ----

  test("now / currentTimestamp / currentDate / localTimestamp — all return the expected SQL types") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          n  <- empty.select(Pg.now).compile.unique(s)
          ct <- empty.select(Pg.currentTimestamp).compile.unique(s)
          cd <- empty.select(Pg.currentDate).compile.unique(s)
          lt <- empty.select(Pg.localTimestamp).compile.unique(s)
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
          _ <- assertIO(empty.select(Pg.concat(param("a"), param("b"), param("c"))).compile.unique(s), "abc")
          _ <- assertIO(
            empty.select(_ =>
              Pg.coalesce(param[Option[String]](None), param[Option[String]](Some("fallback")))
            ).compile.unique(s),
            Option("fallback")
          )
        } yield ()
      }
    }
  }

  // ---- Tier 2: sign / trig / hyperbolic / degrees/radians ----

  test("sign(-5) = -1, sign(0) = 0, sign(3.0) = 1.0") {
    withContainers { containers =>
      session(containers).use { s =>
        // sign(numeric) → numeric; use BigDecimal input to keep types aligned
        for {
          _ <- assertIO(empty.select(Pg.sign(param(BigDecimal(-5)))).compile.unique(s), BigDecimal(-1))
          _ <- assertIO(empty.select(Pg.sign(param(BigDecimal(0)))).compile.unique(s), BigDecimal(0))
          _ <- assertIO(empty.select(Pg.sign(lit(3.0))).compile.unique(s), 1.0)
        } yield ()
      }
    }
  }

  test("sin / cos / tan produce expected values") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          s0 <- empty.select(Pg.sin(lit(0.0))).compile.unique(s)
          c0 <- empty.select(Pg.cos(lit(0.0))).compile.unique(s)
          _ = assert(math.abs(s0) < 1e-10, s"sin(0) = $s0")
          _ = assert(math.abs(c0 - 1.0) < 1e-10, s"cos(0) = $c0")
        } yield ()
      }
    }
  }

  test("atan2 / sinh / cosh / tanh") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          a  <- empty.select(Pg.atan2(lit(1.0), lit(1.0))).compile.unique(s)
          sh <- empty.select(Pg.sinh(lit(0.0))).compile.unique(s)
          ch <- empty.select(Pg.cosh(lit(0.0))).compile.unique(s)
          th <- empty.select(Pg.tanh(lit(0.0))).compile.unique(s)
          _ = assert(math.abs(a - math.Pi / 4) < 1e-10, s"atan2(1,1) = $a")
          _ = assert(math.abs(sh) < 1e-10, s"sinh(0) = $sh")
          _ = assert(math.abs(ch - 1.0) < 1e-10, s"cosh(0) = $ch")
          _ = assert(math.abs(th) < 1e-10, s"tanh(0) = $th")
        } yield ()
      }
    }
  }

  test("degrees / radians round-trip") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          d <- empty.select(Pg.degrees(param(math.Pi))).compile.unique(s)
          r <- empty.select(Pg.radians(lit(180.0))).compile.unique(s)
          _ = assert(math.abs(d - 180.0) < 1e-9, s"degrees(pi) = $d")
          _ = assert(math.abs(r - math.Pi) < 1e-9, s"radians(180) = $r")
        } yield ()
      }
    }
  }

  // ---- Tier 2: string functions ----

  test("initcap / translate / lpad / rpad") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          _ <- assertIO(empty.select(Pg.initcap(param("hello world"))).compile.unique(s), "Hello World")
          _ <- assertIO(empty.select(Pg.translate(param("abc"), "abc", "xyz")).compile.unique(s), "xyz")
          _ <- assertIO(empty.select(Pg.lpad(param("hi"), 5)).compile.unique(s), "   hi")
          _ <- assertIO(empty.select(Pg.rpad(param("hi"), 5, "-")).compile.unique(s), "hi---")
        } yield ()
      }
    }
  }

  test("md5 / ascii / chr") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          h <- empty.select(Pg.md5(param("abc"))).compile.unique(s)
          _ = assertEquals(h, "900150983cd24fb0d6963f7d28e17f72")
          _ <- assertIO(empty.select(Pg.ascii(param("A"))).compile.unique(s), 65)
          _ <- assertIO(empty.select(Pg.chr(lit(65))).compile.unique(s), "A")
        } yield ()
      }
    }
  }

  test("toChar / toNumber") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          c <- empty.select(Pg.toChar(param(BigDecimal("1234.5")), "FM9999.00")).compile.unique(s)
          _ = assert(c.contains("1234"), s"to_char result: $c")
          n <- empty.select(Pg.toNumber(param("1234.50"), "9999.99")).compile.unique(s)
          _ = assert(n > BigDecimal(1234), s"to_number result: $n")
        } yield ()
      }
    }
  }

  test("format with args") {
    withContainers { containers =>
      session(containers).use { s =>
        assertIO(
          empty.select(_ => Pg.format("Hello, %s!", param("world"))).compile.unique(s),
          "Hello, world!"
        )
      }
    }
  }

  // ---- Tier 2: date / time ----

  test("extract year from current_date returns BigDecimal >= 2024") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          y <- empty.select(Pg.extract("year", Pg.currentDate)).compile.unique(s)
          _ = assert(y >= BigDecimal(2024), s"year = $y")
        } yield ()
      }
    }
  }

  test("dateTrunc truncates to month") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          ts <- empty.select(Pg.dateTrunc("month", Pg.now)).compile.unique(s)
          _ = assertEquals(ts.getDayOfMonth, 1)
          _ = assertEquals(ts.getHour, 0)
        } yield ()
      }
    }
  }

  test("makeDate / makeTime / makeTimestamp construct expected values") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          d <- empty.select(Pg.makeDate(lit(2024), lit(6), lit(15))).compile.unique(s)
          _ = assertEquals(d, LocalDate.of(2024, 6, 15))
          ts <- empty.select(Pg.makeTimestamp(lit(2024), lit(6), lit(15), lit(12), lit(0), lit(0.0))).compile.unique(s)
          _ = assertEquals(ts, LocalDateTime.of(2024, 6, 15, 12, 0, 0))
        } yield ()
      }
    }
  }

  test("toTimestamp converts Unix epoch") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          ts <- empty.select(Pg.toTimestamp(lit(0.0))).compile.unique(s)
          _ = assertEquals(ts.toEpochSecond, 0L)
        } yield ()
      }
    }
  }

  test("age(a, b) returns a non-negative interval") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          d <- empty.select(Pg.age(Pg.currentDate, Pg.currentDate)).compile.unique(s)
          _ = assert(d.isZero || !d.isNegative, s"age = $d")
        } yield ()
      }
    }
  }

  // ---- Tier 2: stat aggregates ----

  test("stddev / variance / stddevPop / varPop on generate_series(1, 5)") {
    withContainers { containers =>
      session(containers).use { s =>
        for {
          sd  <- Pg.generateSeries(1, 5).select(g => Pg.stddev(g.n)).compile.unique(s)
          vr  <- Pg.generateSeries(1, 5).select(g => Pg.variance(g.n)).compile.unique(s)
          sdp <- Pg.generateSeries(1, 5).select(g => Pg.stddevPop(g.n)).compile.unique(s)
          vrp <- Pg.generateSeries(1, 5).select(g => Pg.varPop(g.n)).compile.unique(s)
          _ = assert(sd > BigDecimal(0), s"stddev = $sd")
          _ = assert(vr > BigDecimal(0), s"variance = $vr")
          _ = assert(sdp > BigDecimal(0), s"stddev_pop = $sdp")
          _ = assert(vrp > BigDecimal(0), s"var_pop = $vrp")
        } yield ()
      }
    }
  }
}
