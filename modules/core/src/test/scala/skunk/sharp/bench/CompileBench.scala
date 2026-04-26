package skunk.sharp.bench

import skunk.sharp.dsl.*

import java.util.UUID

/**
 * Rough micro-benchmark / dynamic-AF accounting: runs `.compile` on representative SELECT, INSERT,
 * UPDATE, DELETE, and JOIN shapes and reports per-call-site dynamic-AppliedFragment counts plus rough
 * timing. Not JMH — run manually with `sbt "core/runMain skunk.sharp.bench.CompileBench"`. Good enough for
 * before/after sanity checks while we push work from runtime into the macro / interning path.
 *
 * The dynamic-AF count is the load-bearing metric: each `RawConstants.rawDynamic` invocation indicates a
 * compile-time-unknown SQL fragment that allocates a fresh `AppliedFragment`. Driving the per-compile
 * count to zero (outside `whereRaw` / `havingRaw` payloads) is the design goal of this PR.
 */
object CompileBench {

  case class User(id: UUID, email: String, age: Int, createdAt: java.time.OffsetDateTime)
  case class Post(id: UUID, authorId: UUID, title: String, body: String)

  val users = Table.of[User]("users").withPrimary("id").withDefault("id")
  val posts = Table.of[Post]("posts").withPrimary("id").withDefault("id")

  // ---- Scenarios -----------------------------------------------------------------------------------

  def selectWhereOrderLimit(age: Int, email: String): skunk.AppliedFragment =
    users.select
      .where(u => u.age >= age && u.email.like(email))
      .orderBy(u => u.createdAt.desc)
      .limit(20)
      .compile
      .af

  def insertRow(id: UUID, email: String, age: Int, ts: java.time.OffsetDateTime): skunk.AppliedFragment =
    users.insert((id = id, email = email, age = age, createdAt = ts)).compile.af

  def updateSet(id: UUID, email: String): skunk.AppliedFragment =
    users.update.set(u => u.email := email).where(u => u.id === id).compile.af

  def deleteWhere(id: UUID): skunk.AppliedFragment =
    users.delete.where(u => u.id === id).compile.af

  def joinSelect(age: Int): skunk.AppliedFragment =
    users.innerJoin(posts).on(r => r.users.id ==== r.posts.authorId)
      .where(r => r.users.age >= age)
      .select(r => (r.users.email, r.posts.title))
      .compile.af

  // ---- Driver --------------------------------------------------------------------------------------

  def runScenario(name: String, n: Int, body: Int => skunk.AppliedFragment): Unit = {
    val counter = skunk.sharp.internal.RawConstants.rawDynamicCount
    // Warmup
    for (i <- 0 until math.min(n / 10, 50_000)) {
      val af = body(i)
      val _  = af.fragment.sql.length
    }
    // Per-call-site attribution over a small batch
    val csTable = new java.util.concurrent.ConcurrentHashMap[String, Long]()
    skunk.sharp.internal.RawConstants.rawDynamicByCallSite = csTable
    for (i <- 0 until 1000) {
      val af = body(i)
      val _  = af.fragment.sql.length
    }
    skunk.sharp.internal.RawConstants.rawDynamicByCallSite = null

    System.gc(); Thread.sleep(50)
    val rt          = Runtime.getRuntime
    val freeBefore  = rt.freeMemory()
    val totalBefore = rt.totalMemory()
    val rdBefore    = counter.get()
    val t0          = System.nanoTime()
    var chk = 0L
    for (i <- 0 until n) {
      val af = body(i)
      chk ^= af.fragment.sql.length
    }
    val t1 = System.nanoTime()

    val freeAfter   = rt.freeMemory()
    val totalAfter  = rt.totalMemory()
    val rdAfter     = counter.get()
    val elapsed     = (t1 - t0) / 1e6
    val opsPerMs    = n / elapsed
    val allocated   = (totalAfter - totalBefore) + (freeBefore - freeAfter)
    val dynAfs      = rdAfter - rdBefore

    println(s"== $name ==  [checksum=$chk]")
    println(f"  compile × $n: $elapsed%.0f ms → $opsPerMs%.0f ops/ms (~${1000 / opsPerMs}%.2f us/op)")
    println(f"  approx heap delta: ${allocated / 1024}%d KB (${allocated / n}%d bytes/op)")
    println(f"  dynamic AFs: $dynAfs total → ${dynAfs.toDouble / n}%.2f per compile")
    if (!csTable.isEmpty) {
      println("  call sites (per 1000 compiles):")
      import scala.jdk.CollectionConverters.*
      csTable.entrySet.iterator.asScala.toList.sortBy(-_.getValue).foreach { e =>
        println(f"    ${e.getValue}%5d  ${e.getKey}")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val n = 200_000
    val ts = java.time.OffsetDateTime.now()
    runScenario("SELECT u.* WHERE … ORDER BY … LIMIT 20", n, i =>
      selectWhereOrderLimit(18 + (i & 31), "%@example.com"))
    runScenario("INSERT into users (full row)", n, i =>
      insertRow(new UUID(0L, i.toLong), s"u$i@example.com", 18 + (i & 31), ts))
    runScenario("UPDATE users SET email = … WHERE id = …", n, i =>
      updateSet(new UUID(0L, i.toLong), s"u$i@example.com"))
    runScenario("DELETE FROM users WHERE id = …", n, i =>
      deleteWhere(new UUID(0L, i.toLong)))
    runScenario("SELECT … FROM users INNER JOIN posts ON …", n, i =>
      joinSelect(18 + (i & 31)))
  }

}
