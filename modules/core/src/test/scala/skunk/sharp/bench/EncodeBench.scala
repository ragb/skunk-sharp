package skunk.sharp.bench

import skunk.sharp.dsl.*
import java.util.UUID

/** Execute-path micro-benchmark: queries compiled once; args encoded repeatedly (what every execute does). */
object EncodeBench {
  case class User(id: UUID, email: String, age: Int)
  case class Post(id: UUID, user_id: UUID, title: String)
  val users = Table.of[User]("users")
  val posts = Table.of[Post]("posts")

  val q1 = users.select.where(u => u.age >= Param[Int]).compile
  val q3 = users.select.where(u => u.age >= Param[Int] && u.email === Param[String] && u.id === Param[UUID]).compile
  val qp = users.select(u => (u.email, u.age)).where(u => u.age >= Param[Int]).orderBy(u => u.age.desc).limit(5).compile

  val qj = users.innerJoin(posts).on(r => r.users.id === r.posts.user_id && r.posts.title === Param[String])
    .select(r => (r.users.email, r.posts.title)).where(r => r.users.age >= Param[Int]).compile

  val qu = users.update.set(u => u.age := Param[Int]).where(u => u.id === Param[UUID]).compile
  val qi = users.insert.withParams((id = Param[UUID], email = Param[String], age = Param[Int])).compile
  val qv = users.select.where(u => u.age >= 18).compile

  private val id = new UUID(1L, 2L)

  def run(name: String, n: Int)(f: Int => Int): Unit = {
    var chk = 0; var i = 0
    while (i < n / 5) { chk += f(i); i += 1 } // warm-up
    val mx  = java.lang.management.ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]
    val tid = Thread.currentThread.getId
    val a0  = mx.getThreadAllocatedBytes(tid); val t0 = System.nanoTime
    i = 0
    while (i < n) { chk += f(i); i += 1 }
    val t1 = System.nanoTime; val a1 = mx.getThreadAllocatedBytes(tid)
    println(f"$name%-18s ${(t1 - t0).toDouble / n}%7.1f ns/op  ${(a1 - a0) / n}%5d B/op  [chk=$chk]")
  }

  def main(args: Array[String]): Unit = {
    val n = 2000000
    for (round <- 1 to 3) {
      println(s"-- round $round")
      run("void", n)(_ => qv.fragment.encoder.encode(skunk.Void).size)
      run("where1", n)(i => q1.fragment.encoder.encode(i).size)
      run("where3", n)(i => q3.fragment.encoder.encode((i, "x", id)).size)
      run("projWhereOrder", n)(i => qp.fragment.encoder.encode(i).size)
      run("joinOnWhere", n)(i => qj.fragment.encoder.encode(("t", i)).size)
      run("update", n)(i => qu.fragment.encoder.encode((i, id)).size)
      run("insertParams", n)(i => qi.fragment.encoder.encode((id, "e", i)).size)
    }
  }

}
