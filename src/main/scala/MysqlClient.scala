import java.sql._
import scala.util.Random
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

object MysqlClient extends App with Requests {
  val schema = "test"
  val ip = "oscobai052s.sys.meshcore.net"
  val ip2 = "oscobai151s.sys.meshcore.net"
//  val DB_URL = s"jdbc:mysql://$ip:3306/$schema?cacheResultsetMetadata=true"
  val DB_URL = s"jdbc:mysql:loadbalance://$ip:3306,$ip2:3306/$schema?cacheResultsetMetadata=true"
  val USER = "root"
  val PASS = "root"

  val parallelism = 32
  val insertCount = 100000
  val selectCount = 100000

  val conn = connect
  execute(dropTable, conn)
  execute(createTable, conn)

  time(insertN(insertCount), s"insert $insertCount records")
  time(selectN(selectCount), s"select $selectCount records")

  def insertN(count: Int) = {
    val keys = Seq("ID", "DSC")
    val sqlLines = time(Vector.tabulate(insertCount) { i =>
      val v = Seq(i.toString, "desc" + i)
      insert(keys, v)
    }, s"generate $count values").par
    sqlLines.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
    val connections = collection.mutable.Map.empty[Long, Connection]
    for (sql <- sqlLines) {
      val c = connections.getOrElseUpdate(Thread.currentThread.getId, connect)
      execute(sql, c)
    }
  }

  def selectN(count: Int) = {
    val ids = (0 until selectCount).par
    ids.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))
    val connections = collection.mutable.Map.empty[Long, Connection]
    for (i <- ids) {
      val c = connections.getOrElseUpdate(Thread.currentThread.getId, connect)
      val s = conn.prepareStatement(s"select dsc from $tableName where id=?")
      s.setInt(1, i)
      val rs = s.executeQuery()
      rs.next()
      assert(rs.getString(1).startsWith("desc"))
    }
  }

  def connect = {
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    println("Connecting to database...")
    val conn = DriverManager.getConnection(DB_URL, USER, PASS)
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
  val engine = "NDB"
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