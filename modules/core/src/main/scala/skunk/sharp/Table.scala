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

import skunk.sharp.internal.{CompileChecks, DeriveColumns}

import scala.annotation.unused
import scala.compiletime.constValueTuple
import scala.deriving.Mirror

/**
 * A writable Postgres relation (a `BASE TABLE`).
 *
 * `Cols` is a heterogeneous tuple of [[Column]]s describing the table's shape. It is the authoritative source of truth
 * for column names, types, nullability, and declared attributes; the DSL's match types consume it for compile-time
 * query checking.
 *
 * `Name` is the singleton type of the table's name — carried so JOIN extensions can default the alias to the table's
 * name when the user didn't supply an explicit `.alias(...)`.
 *
 * SELECT is available via the shared [[Relation]] extension. INSERT / UPDATE / DELETE are extensions on `Table`
 * specifically — attempting them on a [[View]] is a compile error.
 */
final case class Table[Cols <: Tuple, Name <: String & Singleton](
  name: Name,
  schema: Option[String],
  columns: Cols
) extends Relation[Cols] {

  val expectedTableType: String = "BASE TABLE"

  /** Place the table in a non-default schema. */
  def inSchema(s: String): Table[Cols, Name] = copy(schema = Some(s))

  /**
   * Primitive: rewrite one column's metadata with `f`. The lambda receives the column at its runtime erasure
   * (`Column[Any, N, Boolean, Tuple]`) and must return a column of the same shape.
   *
   * Unknown names produce a friendly compile error. Use [[withPrimary]] / [[withUnique]] / [[withDefault]] for the
   * common attribute modifiers — those also flip the corresponding `Attrs`-tuple markers on `Cols`.
   */
  inline def withColumn[N <: String & Singleton](inline n: N)(
    f: Column[Any, N, Boolean, Tuple] => Column[Any, N, Boolean, Tuple]
  ): Table[Cols, Name] = {
    CompileChecks.requireColumn[Cols, N]
    copy(columns = Table.updateCol[Cols, N](columns, n, f).asInstanceOf[Cols])
  }

  /**
   * Mark a column as a single-column primary key. Appends a `ColumnAttr.Pk[(N)]` marker so `.onConflict(c => c.<N>)` is
   * accepted at compile time via [[HasUniqueness]].
   *
   * For composite primary keys — where the PK spans multiple columns — use [[withCompositePrimary]] instead.
   */
  inline def withPrimary[N <: String & Singleton](inline n: N)
    : Table[Table.AddAttr[Cols, N, ColumnAttr.Pk[N *: EmptyTuple]], Name] = {
    CompileChecks.requireColumn[Cols, N]
    val nameStr = compiletime.constValue[N]: String
    val updated = Table.updateCol[Cols, N](
      columns,
      n,
      c => c.copy(attrs = c.attrs :+ ColumnAttrValue.Pk(List(nameStr)))
    )
    copy(columns = updated.asInstanceOf[Table.AddAttr[Cols, N, ColumnAttr.Pk[N *: EmptyTuple]]])
      .asInstanceOf[Table[Table.AddAttr[Cols, N, ColumnAttr.Pk[N *: EmptyTuple]], Name]]
  }

  /**
   * Mark several columns as jointly making up a composite primary key. Each listed column receives `ColumnAttr.Pk[Ns]`
   * (the full tuple of PK column names), so composite `.onConflictComposite(c => (c.a, c.b))` checks for exact
   * set-equality against `Ns` via [[HasCompositeUniqueness]].
   *
   * Column names are passed as a **type argument** — a tuple type of string literals. Type arguments preserve literal
   * singletons (value-level tuple literals would widen to `(String, String)` and break the match-type machinery).
   *
   * {{{
   *   Table.of[Order]("orders").withCompositePrimary[("tenant_id", "order_id")]
   * }}}
   */
  inline def withCompositePrimary[Ns <: NonEmptyTuple](using
    @unused ev: Tuple.Union[Ns] <:< (String & Singleton)
  ): Table[Table.AddCompositePk[Cols, Ns], Name] = {
    CompileChecks.requireAllNamesInCols[Cols, Ns]
    val names   = constValueTuple[Ns].toList.asInstanceOf[List[String]]
    val nameSet = names.toSet
    val marker  = ColumnAttrValue.Pk(names)
    val updated =
      Table.mapCols(
        columns,
        (c: Column[Any, String & Singleton, Boolean, Tuple]) =>
          if (nameSet.contains(c.name)) c.copy(attrs = c.attrs :+ marker) else c
      )
    copy(columns = updated.asInstanceOf[Table.AddCompositePk[Cols, Ns]])
      .asInstanceOf[Table[Table.AddCompositePk[Cols, Ns], Name]]
  }

  /**
   * Mark a column as having a single-column `UNIQUE` constraint. Appends `ColumnAttr.Uq[N, (N)]` (the constraint's
   * "name" is the column name itself, matching Postgres's default naming) so `.onConflict(c => c.<N>)` accepts this
   * column.
   *
   * For composite unique indexes (one constraint spanning multiple columns), use [[withUniqueIndex]].
   */
  inline def withUnique[N <: String & Singleton](inline n: N)
    : Table[Table.AddAttr[Cols, N, ColumnAttr.Uq[N, N *: EmptyTuple]], Name] = {
    CompileChecks.requireColumn[Cols, N]
    val nameStr = compiletime.constValue[N]: String
    val updated = Table.updateCol[Cols, N](
      columns,
      n,
      c => c.copy(attrs = c.attrs :+ ColumnAttrValue.Uq(nameStr, List(nameStr)))
    )
    copy(columns = updated.asInstanceOf[Table.AddAttr[Cols, N, ColumnAttr.Uq[N, N *: EmptyTuple]]])
      .asInstanceOf[Table[Table.AddAttr[Cols, N, ColumnAttr.Uq[N, N *: EmptyTuple]], Name]]
  }

  /**
   * Mark several columns as jointly making up a named `UNIQUE` constraint. Each listed column receives
   * `ColumnAttr.Uq[ConstraintName, Ns]`, so composite `.onConflictComposite(c => (c.a, c.b))` is accepted as long as
   * its column set is set-equal to `Ns`.
   *
   * Both arguments are **type arguments** — string literals as type parameters preserve their singleton types; the
   * corresponding value-level literals would widen. `ConstraintName` is the SQL constraint name (surfaced by the schema
   * validator in `Mismatch` messages).
   *
   * {{{
   *   Table.of[Order]("orders").withUniqueIndex["uq_orders_tenant_slug", ("tenant_id", "slug")]
   * }}}
   */
  inline def withUniqueIndex[ConstraintName <: String & Singleton, Ns <: NonEmptyTuple](using
    @unused ev: Tuple.Union[Ns] <:< (String & Singleton)
  ): Table[Table.AddCompositeUq[Cols, ConstraintName, Ns], Name] = {
    CompileChecks.requireAllNamesInCols[Cols, Ns]
    val cname   = compiletime.constValue[ConstraintName]: String
    val names   = constValueTuple[Ns].toList.asInstanceOf[List[String]]
    val nameSet = names.toSet
    val marker  = ColumnAttrValue.Uq(cname, names)
    val updated =
      Table.mapCols(
        columns,
        (c: Column[Any, String & Singleton, Boolean, Tuple]) =>
          if (nameSet.contains(c.name)) c.copy(attrs = c.attrs :+ marker) else c
      )
    copy(columns = updated.asInstanceOf[Table.AddCompositeUq[Cols, ConstraintName, Ns]])
      .asInstanceOf[Table[Table.AddCompositeUq[Cols, ConstraintName, Ns], Name]]
  }

  /**
   * Override a column's skunk codec. The column's `tpe` (skunk `data.Type`) is derived from the codec, so pass
   * `varchar(256)` / `int8` / `uuid` (from `skunk.codec.all`) directly. The codec's value type `T` must match the
   * column's declared Scala value type (not statically checked because the match type doesn't reduce here; a mismatch
   * raises at the first row that encodes or decodes).
   */
  inline def withColumnCodec[N <: String & Singleton, T](inline n: N, codec: skunk.Codec[T]): Table[Cols, Name] =
    withColumn(n)(c =>
      c.copy(
        tpe = skunk.sharp.pg.PgTypes.typeOf(codec),
        codec = codec.asInstanceOf[skunk.Codec[Any]]
      )
    )

  /**
   * Mark a column as having a database-side default. Adds the [[ColumnAttr.Default]] marker to that column's `Attrs` so
   * INSERTs that omit the column become legal at the type level.
   */
  inline def withDefault[N <: String & Singleton](inline n: N)
    : Table[Table.AddAttr[Cols, N, ColumnAttr.Default], Name] = {
    CompileChecks.requireColumn[Cols, N]
    val updated = Table.updateCol[Cols, N](
      columns,
      n,
      c => c.copy(attrs = c.attrs :+ ColumnAttrValue.Default)
    )
    copy(columns = updated.asInstanceOf[Table.AddAttr[Cols, N, ColumnAttr.Default]])
      .asInstanceOf[Table[Table.AddAttr[Cols, N, ColumnAttr.Default], Name]]
  }

  /**
   * PK columns — the full member list, in the declaration order stored on the `Pk` marker. Used by
   * [[skunk.sharp.validation]] to diff against `information_schema.table_constraints`. Returns `Nil` if no column has a
   * `Pk` marker.
   */
  def pkColumns: List[String] = {
    val cols = columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    cols.iterator
      .flatMap(_.attrs.collectFirst { case ColumnAttrValue.Pk(ms) => ms })
      .nextOption()
      .getOrElse(Nil)
  }

  /**
   * UNIQUE constraints as `name → columns` pairs. Each `Uq(name, members)` marker on any participating column carries
   * the full group identity; duplicates across columns of the same group collapse.
   */
  def uniqueIndexes: Map[String, List[String]] = {
    val cols = columns.toList.asInstanceOf[List[Column[?, ?, ?, ?]]]
    cols.iterator
      .flatMap(_.attrs.collect { case ColumnAttrValue.Uq(n, ms) => n -> ms })
      .toMap
  }

}

object Table {

  /** Entry point for the column-by-column builder path. */
  inline def builder[Name <: String & Singleton](name: Name): TableBuilder[EmptyTuple, Name] =
    new TableBuilder[EmptyTuple, Name](name, None, EmptyTuple)

  /**
   * Derive a [[Table]] from a case class via its `Mirror.ProductOf`. Only the case class's *shape* (field labels and
   * types) is used; the case class type itself is not remembered by `Table` — use `.to[User]` on a query to materialise
   * rows as `User` when you want to.
   *
   * Returns a continuation so the call site `Table.of[User]("users")` can specify `T` explicitly while `Name` is still
   * inferred from the name literal — Scala's all-or-nothing type-parameter inference forbids doing both in one call.
   */
  inline def of[T <: Product]: OfCont[T] = new OfCont[T]

  /** Continuation for [[of]]. Kept public because the builder / test code refers to it by path. */
  final class OfCont[T <: Product] {

    inline def apply[Name <: String & Singleton](tableName: Name)(using
      m: Mirror.ProductOf[T],
      dc: DeriveColumns[m.MirroredElemLabels, m.MirroredElemTypes]
    ): Table[dc.Out, Name] = Table[dc.Out, Name](tableName, None, dc.value)

  }

  /** Type-level: append an attribute marker `A` to the `Attrs` tuple of the column named `N`. */
  type AddAttr[Cols <: Tuple, N <: String & Singleton, A] <: Tuple = Cols match {
    case Column[t, N, nu, attrs] *: tail => Column[t, N, nu, Tuple.Append[attrs, A]] *: tail
    case h *: tail                       => h *: AddAttr[tail, N, A]
    case EmptyTuple                      => EmptyTuple
  }

  /**
   * Type-level: append `ColumnAttr.Pk[Ns]` to every column whose name is in `Ns`. All listed columns end up with the
   * same `Pk[Ns]` marker, which is what [[HasCompositeUniqueness]] matches against.
   */
  type AddCompositePk[Cols <: Tuple, Ns <: Tuple] <: Tuple = Cols match {
    case Column[t, n, nu, attrs] *: tail => Contains[n, Ns] match {
        case true  => Column[t, n, nu, Tuple.Append[attrs, ColumnAttr.Pk[Ns]]] *: AddCompositePk[tail, Ns]
        case false => Column[t, n, nu, attrs] *: AddCompositePk[tail, Ns]
      }
    case EmptyTuple => EmptyTuple
  }

  /**
   * Type-level: append `ColumnAttr.Uq[Name, Ns]` to every column whose name is in `Ns`. Parallels [[AddCompositePk]]
   * but for named `UNIQUE` constraints.
   */
  type AddCompositeUq[Cols <: Tuple, Name <: String & Singleton, Ns <: Tuple] <: Tuple = Cols match {
    case Column[t, n, nu, attrs] *: tail => Contains[n, Ns] match {
        case true  => Column[t, n, nu, Tuple.Append[attrs, ColumnAttr.Uq[Name, Ns]]] *: AddCompositeUq[tail, Name, Ns]
        case false => Column[t, n, nu, attrs] *: AddCompositeUq[tail, Name, Ns]
      }
    case EmptyTuple => EmptyTuple
  }

  /** Runtime helper: walk the columns tuple, apply `f` to the column whose singleton-name matches `n`. */
  private[sharp] def updateCol[Cols <: Tuple, N <: String & Singleton](
    cols: Cols,
    n: N,
    f: Column[Any, N, Boolean, Tuple] => Column[Any, N, Boolean, Tuple]
  ): Tuple = {
    val updated = cols.toList.map {
      case c: Column[?, ?, ?, ?] if c.name == n =>
        f(c.asInstanceOf[Column[Any, N, Boolean, Tuple]])
      case other => other
    }
    Tuple.fromArray(updated.toArray[Any])
  }

  /** Runtime helper: apply `f` to every column in the tuple. */
  private[sharp] def mapCols(
    cols: Tuple,
    f: Column[Any, String & Singleton, Boolean, Tuple] => Column[Any, String & Singleton, Boolean, Tuple]
  ): Tuple = {
    val updated = cols.toList.map {
      case c: Column[?, ?, ?, ?] => f(c.asInstanceOf[Column[Any, String & Singleton, Boolean, Tuple]])
      case other                 => other
    }
    Tuple.fromArray(updated.toArray[Any])
  }

}
