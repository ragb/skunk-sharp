package skunk.sharp.internal

import cats.syntax.all.*
import skunk.Codec
import skunk.data.{Encoded, Type}
import skunk.sharp.{Column, ValuesOf}

/**
 * Holds `rowCodec` and `tupleCodec`. Wrapped in an explicit object (rather than top-level `def`s) because Scala 3
 * represents top-level defs in a synthetic "module class" named after the package; inline methods that reference these
 * from outside the package emit TASTy references to that module class. Test compiles then see "Warning: mocking up
 * superclass for module class internal" and fail at runtime with `NoClassDefFoundError: skunk/sharp/internal`. An
 * explicit object avoids the synthetic module class entirely.
 */
object RowCodecs {

  def rowCodec[Cols <: Tuple](cols: Cols): Codec[ValuesOf[Cols]] =
    (cols: Tuple) match {
      case EmptyTuple =>
        RowCodec.empty.asInstanceOf[Codec[ValuesOf[Cols]]]
      case head *: tail =>
        val col       = head.asInstanceOf[Column[?, ?, ?, ?]]
        val tailCodec = rowCodec(tail)
        RowCodec.cons(col.codec, tailCodec).asInstanceOf[Codec[ValuesOf[Cols]]]
    }

  def tupleCodec(codecs: List[Codec[?]]): Codec[Tuple] =
    codecs.foldRight(RowCodec.empty.asInstanceOf[Codec[Tuple]]) { (c, acc) =>
      RowCodec.cons(c.asInstanceOf[Codec[Any]], acc).asInstanceOf[Codec[Tuple]]
    }

}

private object RowCodec {

  /** Codec for the empty tuple — encodes nothing, decodes an empty row. */
  val empty: Codec[EmptyTuple] =
    new Codec[EmptyTuple] {
      def encode(a: EmptyTuple): List[Option[Encoded]]                                           = Nil
      def decode(offset: Int, ss: List[Option[String]]): Either[skunk.Decoder.Error, EmptyTuple] =
        EmptyTuple.asRight
      val types: List[Type]                 = Nil
      val sql: cats.data.State[Int, String] = "".pure[cats.data.State[Int, *]]
    }

  /**
   * Cons a head codec onto a tail codec. Mirrors skunk's internal `*:` on tuple-shaped codecs, but we open-code it here
   * to avoid depending on the `skunk.syntax.codec` private member.
   */
  def cons[H, T <: Tuple](h: Codec[H], t: Codec[T]): Codec[H *: T] =
    new Codec[H *: T] {
      def encode(a: H *: T): List[Option[Encoded]] = h.encode(a.head) ++ t.encode(a.tail)
      def decode(offset: Int, ss: List[Option[String]]): Either[skunk.Decoder.Error, H *: T] =
        for {
          hv <- h.decode(offset, ss.take(h.types.length))
          tv <- t.decode(offset + h.types.length, ss.drop(h.types.length))
        } yield hv *: tv
      val types: List[Type]                 = h.types ++ t.types
      val sql: cats.data.State[Int, String] =
        for {
          hs <- h.sql
          ts <- t.sql
        } yield if ts.isEmpty then hs else s"$hs, $ts"
    }

}
