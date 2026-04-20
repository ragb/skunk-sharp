package skunk.sharp.circe

import io.circe.Json as CirceJson
import skunk.Codec
import skunk.circe.codec.all as circeCodecs

/**
 * Re-export the `json` and `jsonb` codecs from skunk-circe under names the tag companions in this module pick up. Kept
 * in its own file so future additions (typed variants parametrised by a circe `Encoder` / `Decoder`, etc.) have an
 * obvious home.
 */
private[circe] object codecs {

  val jsonb: Codec[CirceJson] = circeCodecs.jsonb
  val json: Codec[CirceJson]  = circeCodecs.json

}
