package skunk.sharp.json

import io.circe.Json as CirceJson
import io.circe.parser.parse
import skunk.Codec
import skunk.data.Type

/**
 * Minimal json / jsonb codecs for [[io.circe.Json]], lifted inline so the module doesn't depend on skunk-circe (which
 * tracks skunk milestones and clashes with our skunk 1.0.0 stable). Pure circe + skunk — no extra transitive deps.
 *
 * Encoder emits compact JSON text; decoder parses with circe and surfaces any parse error as a decoder failure.
 */
private[json] object codecs {

  val jsonb: Codec[CirceJson] = Codec.simple[CirceJson](
    _.noSpaces,
    s => parse(s).left.map(_.getMessage),
    Type.jsonb
  )

  val json: Codec[CirceJson] = Codec.simple[CirceJson](
    _.noSpaces,
    s => parse(s).left.map(_.getMessage),
    Type.json
  )

}
