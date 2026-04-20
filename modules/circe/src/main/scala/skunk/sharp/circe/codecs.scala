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
