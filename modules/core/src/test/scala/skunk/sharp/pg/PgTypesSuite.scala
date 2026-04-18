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
