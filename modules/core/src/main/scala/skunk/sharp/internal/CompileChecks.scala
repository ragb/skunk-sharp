package skunk.sharp.internal

import skunk.sharp.{Column, HasColumn}

import scala.compiletime.*

/**
 * Quoted-macro-free compile-time helpers that turn the bare "cannot prove `HasColumn[Cols, N] =:= true`" error into a
 * sentence that actually mentions the column name and the columns that exist.
 *
 * Used by `Table`'s `withPrimary` / `withUnique` / `withDefault` modifiers.
 */
object CompileChecks {

  /**
   * Assert at compile time that column `N` exists in `Cols`. On failure, emits an error message listing the available
   * columns. No runtime cost — `error` is resolved during inlining.
   */
  inline def requireColumn[Cols <: Tuple, N <: String & Singleton]: Unit =
    inline if constValue[HasColumn[Cols, N]] then ()
    else
      error(
        "Column \"" + constValue[N] + "\" not found in table. Available columns: [" + columnNamesString[Cols] + "]"
      )

  /**
   * Walks `Cols` at compile time and folds the column names into a comma-separated string. Each step resolves through
   * `constValue`, so the resulting String is a compile-time constant that [[error]] can accept.
   */
  transparent inline def columnNamesString[Cols <: Tuple]: String =
    inline erasedValue[Cols] match {
      case _: EmptyTuple =>
        ""
      case _: (Column[t, n, nu, d] *: EmptyTuple) =>
        constValue[n & String]
      case _: (Column[t, n, nu, d] *: tail) =>
        constValue[n & String] + ", " + columnNamesString[tail]
    }

  /**
   * Resolve a tuple of column names against `Cols` at compile time, returning the list of matching columns in order.
   * Unknown names trip the same friendly compile error as [[requireColumn]].
   */
  inline def lookupColumns[Cols <: Tuple, Names <: Tuple](cols: Cols): List[Column[?, ?, ?, ?]] =
    inline erasedValue[Names] match {
      case _: EmptyTuple =>
        Nil
      case _: (n *: rest) =>
        requireColumn[Cols, n & String & Singleton]
        val name  = constValue[n & String]
        val found =
          cols
            .toList
            .asInstanceOf[List[Column[?, ?, ?, ?]]]
            .find(_.name == name)
            .getOrElse(sys.error(s"skunk-sharp: column $name passed compile check but not found at runtime"))
        found :: lookupColumns[Cols, rest](cols)
    }

  /**
   * Assert every name in `Ns` is a declared column in `Cols`. Error lists the first offender (via per-name
   * [[requireColumn]] recursion).
   */
  inline def requireAllNamesInCols[Cols <: Tuple, Ns <: Tuple]: Unit =
    inline erasedValue[Ns] match {
      case _: EmptyTuple  => ()
      case _: (n *: rest) =>
        requireColumn[Cols, n & String & Singleton]
        requireAllNamesInCols[Cols, rest]
    }

  /**
   * Assert every *required* column (non-defaulted) in `Cols` has its name listed in `Ns`. Error names the first missing
   * required column and gives the caller something actionable.
   */
  inline def requireCoversRequired[Cols <: Tuple, Ns <: Tuple]: Unit =
    inline erasedValue[Cols] match {
      case _: EmptyTuple                        => ()
      case _: (Column[t, n, nu, false] *: tail) =>
        inline if constValue[skunk.sharp.Contains[n, Ns]] then requireCoversRequired[tail, Ns]
        else
          error(
            "skunk-sharp: insert is missing required column \"" + constValue[n & String] +
              "\". Columns without a database default must be present in the row."
          )
      case _: (Column[t, n, nu, true] *: tail) =>
        requireCoversRequired[tail, Ns]
    }

  /**
   * Assert each `Vs`-element is a subtype of its corresponding column's declared value type (looked up by matching name
   * from `Ns`). Produces a standard `cannot prove A <:< B` error pointing at the offending field; imperfect but better
   * than silent runtime failure.
   */
  inline def requireValueTypesMatch[Cols <: Tuple, Ns <: Tuple, Vs <: Tuple]: Unit =
    inline erasedValue[Ns] match {
      case _: EmptyTuple => ()
      case _: (n *: ns)  =>
        inline erasedValue[Vs] match {
          case _: (v *: vs) =>
            summonInline[v <:< skunk.sharp.ColumnType[Cols, n & String & Singleton]]
            requireValueTypesMatch[Cols, ns, vs]
        }
    }

  /**
   * Assert that the column name `N` is NOT already declared in `Cols`. Used by `Table.builder`'s `.column` /
   * `.columnOpt` / `.columnDefaulted` / `.columnOptDefaulted` to catch accidental typos that would otherwise silently
   * add a second column of the same name (e.g. `.column[UUID]("id") … .column[String]("id")`).
   */
  inline def requireColumnAbsent[Cols <: Tuple, N <: String & Singleton]: Unit =
    inline if constValue[HasColumn[Cols, N]] then
      error(
        "skunk-sharp: duplicate column name \"" + constValue[N] +
          "\". Columns already declared in this builder: [" + columnNamesString[Cols] + "]"
      )
    else ()

}
