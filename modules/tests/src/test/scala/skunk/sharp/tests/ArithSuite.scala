package skunk.sharp.tests

import skunk.sharp.dsl.*

import java.time.{Duration, LocalDate, LocalDateTime, OffsetDateTime, ZoneOffset}

/**
 * Infix arithmetic against Postgres. Decoding each result also verifies the promotion table: skunk checks every
 * declared result type against the one Postgres reports, so a wrong `Plus` / `Minus` / … instance fails here.
 */
class ArithSuite extends PgFixture {

  test("numeric promotion matches Postgres's result types, and values are right") {
    withContainers { containers =>
      session(containers).use { s =>
        val q = empty
          .select(_ =>
            (
              Param.named["s", Short] + Param.named["i", Int],         // int4
              Param.named["i", Int] + Param.named["l", Long],          // int8
              Param.named["l", Long] * Param.named["n", BigDecimal],   // numeric
              Param.named["n", BigDecimal] - Param.named["d", Double], // float8
              Param.named["f", Float] + Param.named["f", Float],       // float4
              Param.named["f", Float] * Param.named["d", Double],      // float8
              Param.named["i", Int] / 2,                               // int4, truncating
              Param.named["l", Long] % Param.named["i", Int], // int8
              -Param.named["i", Int]
            )
          )
          .compile
        assertIO(
          q.unique(s)((s = 2.toShort, i = 7, l = 10L, n = BigDecimal("1.5"), d = 0.5, f = 1.5f)),
          (9, 17L, BigDecimal("15.0"), 1.0, 3.0f, 0.75, 3, 3L, -7)
        )
      }
    }
  }

  test("nullable operands give NULL; date and time arithmetic") {
    withContainers { containers =>
      session(containers).use { s =>
        val day   = LocalDate.of(2026, 1, 31)
        val at    = OffsetDateTime.of(2026, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC)
        val local = LocalDateTime.of(2026, 1, 1, 12, 0)
        val q     = empty
          .select(_ =>
            (
              Param.named["maybe", Option[Int]] + 1,
              Param.named["day", LocalDate] + 1,
              Param.named["day", LocalDate] - Param.named["day2", LocalDate],
              Param.named["at", OffsetDateTime] + Param.named["span", Duration],
              Param.named["local", LocalDateTime] - Param.named["span", Duration],
              Param.named["day", LocalDate] + Param.named["span", Duration],
              Param.named["span", Duration] * 2.0
            )
          )
          .compile
        assertIO(
          q.unique(s)((
            maybe = None,
            day = day,
            day2 = LocalDate.of(2026, 1, 1),
            at = at,
            span = Duration.ofHours(36),
            local = local
          )),
          (
            None,
            LocalDate.of(2026, 2, 1),
            30,
            at.plusHours(36),
            local.minusHours(36),
            day.atStartOfDay.plusHours(36),
            Duration.ofHours(72)
          )
        )
      }
    }
  }
}
