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

}
