package skunk.sharp.bench

import skunk.sharp.dsl.*

import java.util.UUID

/**
 * Rough micro-benchmark: time and allocation for a tight loop of `.compile` on a representative
 * SELECT-WHERE-ORDER-LIMIT query. Not JMH — run manually with `sbt "core/runMain skunk.sharp.bench.CompileBench"`.
 * Good enough for before/after sanity checks while we push work from runtime into the macro path.
 */
object CompileBench {

  case class User(id: UUID, email: String, age: Int, createdAt: java.time.OffsetDateTime)

  val users = Table.of[User]("users").withPrimary("id").withDefault("id")

  def runOne(age: Int, email: String): skunk.AppliedFragment =
    users.select
      .where(u => u.age >= age && u.email.like(email))
      .orderBy(u => u.createdAt.desc)
      .limit(20)
      .compile
      .af

  def main(args: Array[String]): Unit = {
    val n = 500_000
    val counter = skunk.sharp.internal.RawConstants.rawDynamicCount

    // Warmup.
    for (_ <- 0 until 50_000) runOne(18, "%@example.com")

    // Capture per-call-site dynamic-AF attribution over a small batch.
    val csTable = new java.util.concurrent.ConcurrentHashMap[String, Long]()
    skunk.sharp.internal.RawConstants.rawDynamicByCallSite = csTable
    for (i <- 0 until 1000) runOne(18 + (i & 31), "%@example.com")
    skunk.sharp.internal.RawConstants.rawDynamicByCallSite = null
    println("--- dynamic-AF call sites (per 1000 compiles) ---")
    import scala.jdk.CollectionConverters.*
    val sortedSites = csTable.entrySet.iterator.asScala.toList.sortBy(-_.getValue)
    sortedSites.foreach(e => println(f"  ${e.getValue}%6d  ${e.getKey}"))
    println("------")

    // Force GC before measurement, then read counters.
    System.gc()
    Thread.sleep(100)
    val rt       = Runtime.getRuntime
    val freeBefore = rt.freeMemory()
    val totalBefore = rt.totalMemory()
    val rdBefore   = counter.get()
    val t0     = System.nanoTime()

    var chk = 0L
    for (i <- 0 until n) {
      val af = runOne(18 + (i & 31), "%@example.com")
      chk ^= af.fragment.sql.length
    }

    val t1         = System.nanoTime()
    val freeAfter  = rt.freeMemory()
    val totalAfter = rt.totalMemory()
    val rdAfter    = counter.get()

    val elapsed  = (t1 - t0) / 1e6
    val opsPerMs = n / elapsed

    // Approx bytes allocated — Runtime metrics are noisy but give a ballpark.
    val allocated = (totalAfter - totalBefore) + (freeBefore - freeAfter)
    val dynAfs    = rdAfter - rdBefore

    println(f"[checksum: $chk]")
    println(f"compile × $n: $elapsed%.1f ms → $opsPerMs%.0f ops/ms (~${1000 / opsPerMs}%.2f us/op)")
    println(f"approx heap delta: ${allocated / 1024}%d KB (${allocated / n}%d bytes/op)")
    println(f"dynamic AFs: $dynAfs total → ${dynAfs.toDouble / n}%.2f per compile")
  }

}
