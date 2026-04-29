package skunk.sharp.pg.functions

import skunk.Void
import skunk.sharp.TypedExpr

/** Session / connection introspection keywords. All Args = Void (no inputs). */
trait PgSession {

  val currentUser:     TypedExpr[String, Void] = TypedExpr(TypedExpr.voidFragment("current_user"),       skunk.codec.all.text)
  val sessionUser:     TypedExpr[String, Void] = TypedExpr(TypedExpr.voidFragment("session_user"),       skunk.codec.all.text)
  val user:            TypedExpr[String, Void] = TypedExpr(TypedExpr.voidFragment("user"),               skunk.codec.all.text)
  val currentSchema:   TypedExpr[String, Void] = TypedExpr(TypedExpr.voidFragment("current_schema"),     skunk.codec.all.text)
  val currentCatalog:  TypedExpr[String, Void] = TypedExpr(TypedExpr.voidFragment("current_catalog"),    skunk.codec.all.text)
  val currentDatabase: TypedExpr[String, Void] = TypedExpr(TypedExpr.voidFragment("current_database()"), skunk.codec.all.text)

}
