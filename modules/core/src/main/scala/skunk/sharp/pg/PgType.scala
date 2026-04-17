/*
 * Copyright 2026 Rui Batista
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package skunk.sharp.pg

/** A Postgres data type, carrying only what the schema validator needs: the value that appears in
  * `information_schema.columns.data_type`.
  *
  * We deliberately do NOT model anything used to generate DDL — users (or migration tools like flyway/dumbo) own the
  * schema. The library only reads the schema, never writes it.
  *
  * The ADT is open via [[PgType.Custom]] so third-party modules (jsonb, ltree, arrays, PostGIS, …) can describe their
  * own types without modifying this file.
  */
sealed trait PgType:
  /** The string that appears in `information_schema.columns.data_type` for columns of this type. */
  def dataType: String

object PgType:

  case object Bool extends PgType  { val dataType = "boolean"  }
  case object Int2 extends PgType  { val dataType = "smallint" }
  case object Int4 extends PgType  { val dataType = "integer"  }
  case object Int8 extends PgType  { val dataType = "bigint"   }
  case object Float4 extends PgType { val dataType = "real"             }
  case object Float8 extends PgType { val dataType = "double precision" }
  case object Numeric extends PgType { val dataType = "numeric" }
  case object Varchar extends PgType { val dataType = "character varying" }
  case object Text extends PgType    { val dataType = "text"   }
  case object Bytea extends PgType   { val dataType = "bytea"  }
  case object Uuid extends PgType    { val dataType = "uuid"   }
  case object Date extends PgType    { val dataType = "date"   }
  case object Time extends PgType        { val dataType = "time without time zone"      }
  case object Timetz extends PgType      { val dataType = "time with time zone"         }
  case object Timestamp extends PgType   { val dataType = "timestamp without time zone" }
  case object Timestamptz extends PgType { val dataType = "timestamp with time zone"    }

  /** Open extension point: user modules describe their Postgres types with `Custom`, supplying the `data_type` string
    * Postgres reports in `information_schema.columns.data_type`.
    */
  final case class Custom(dataType: String) extends PgType
