import java.sql._
import scala.util.Random
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

import com.typesafe.config.ConfigFactory
import java.io.File

object MysqlClient extends App with Requests {
  val conf = ConfigFactory.parseFile(new File("bench.conf"))

  val insertParallelism = conf.getInt("bench.insert.threads")
  val selectParallelism = conf.getInt("bench.select.threads")
  val insertCount = conf.getInt("bench.insert.count")
  val selectCount = conf.getInt("bench.select.count")

  {
    val conn = connect
    execute(dropTable, conn)
    execute(createTable, conn)
  }

  time(insertN(insertCount), s"insert $insertCount records")
  time(selectN(selectCount), s"select $selectCount records")

  def insertN(count: Int) = {
    val keys = Seq("ID", "DSC")
    val sqlLines = time(Vector.tabulate(insertCount) { i =>
      val v = Seq(i.toString, "desc" + i)
      insert(keys, v)
    }, s"generate $count values").par
    sqlLines.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(insertParallelism))
    val connections = collection.mutable.Map.empty[Long, Connection]
    for (sql <- sqlLines) {
      val c = connections.getOrElseUpdate(Thread.currentThread.getId, connect)
      execute(sql, c)
    }
  }

  def selectN(count: Int) = {
    val ids =  Seq.fill(selectCount)(Random.nextInt(insertCount)).par
    ids.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(selectParallelism))
    val connections = collection.mutable.Map.empty[Long, Connection]
    for (i <- ids) {
      val c = connections.getOrElseUpdate(Thread.currentThread.getId, connect)
      val s = c.prepareStatement(s"select dsc from $tableName where id=?")
      s.setInt(1, i)
      val rs = s.executeQuery()
      rs.next()
      assert(rs.getString(1).startsWith("desc"))
    }
  }

  def connect = {
    val url = conf.getString("jdbc.url")
    val user = conf.getString("jdbc.user")
    val password = conf.getString("jdbc.password")
    Class.forName(conf.getString("jdbc.driver")).newInstance()
    println(s"Connecting to database $url")
    val conn = DriverManager.getConnection(url, user, password)
    conn.setAutoCommit(true)
    println("Connected !")
    conn
  }

  def execute(sql: String, conn: Connection) = {
    debug("executing " + sql)
    conn.createStatement().execute(sql)
    debug("done : " + sql)
  }

  def debug(m: => Any) = () //Console.err.println(m)

  def time[T](f: => T, msg: String): T = {
    val start = System.nanoTime
    val res = f
    val end = System.nanoTime
    val dur = (end - start) / 1000000
    println(s"done $msg in $dur ms")
    res
  }
}

trait Requests {
  val engine = "INNODB"
  val tableName = "simple_table"
  val createTable = s"""
CREATE TABLE $tableName (
  `ID` bigint(20) NOT NULL,
  `DSC` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=$engine     """
  val dropTable = s"DROP TABLE if exists $tableName "

  def insert(keys: Iterable[String], values: Iterable[String]) =
    s"insert into $tableName (${keys.mkString(",")}) values (${values.mkString("'", "','", "'")})"

}