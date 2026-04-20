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

package skunk.sharp.pg

import skunk.codec.all.*
import skunk.data.Type

class PgTypesSuite extends munit.FunSuite {

  test("typeOf reads the single skunk type from a single-column codec") {
    assertEquals(PgTypes.typeOf(int4), Type.int4)
    assertEquals(PgTypes.typeOf(varchar(256)), Type.varchar(256))
    assertEquals(PgTypes.typeOf(uuid.opt), Type.uuid)
  }

  test("typeOf rejects a multi-column codec with a message naming the offending types") {
    val multi = int4 ~ text
    val ex    = intercept[IllegalArgumentException](PgTypes.typeOf(multi))
    assert(ex.getMessage.contains("single-column codec"), ex.getMessage)
    assert(ex.getMessage.contains("int4"), ex.getMessage)
    assert(ex.getMessage.contains("text"), ex.getMessage)
  }
}
