package Streaming_Scocker

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}




class Util {
  private var connection: Connection =  null

  private var preparedStatement: PreparedStatement = null

  private var resultSet: ResultSet = null


  private def setPreparedStatement(objects: Array[Any]): Unit = {
    var i = 1
    for (obj <- objects) {
      this.preparedStatement.setObject(i, obj)
      i += 1
    }
  }

  def execute(sql: String): Unit = {
    try {
      this.connection = Util.getConnection
      println(connection)
      this.preparedStatement = this.connection.prepareStatement(sql)
      this.preparedStatement.execute()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      Util.returnConnection(this.connection)
      this.connection == null
    }
  }

  def insertForGeneratedKeys(sql: String, objects: Array[Any]): ResultSet = {
    try {
      this.connection = Util.getConnection
      this.preparedStatement = this.connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
      if (objects != null)
        this.setPreparedStatement(objects)
      this.preparedStatement.executeUpdate()
      this.resultSet = preparedStatement.getGeneratedKeys
    }
    catch {
      case e: Exception => e.printStackTrace()
    } finally {
      Util.returnConnection(this.connection)
      this.connection == null
    }
    this.resultSet
  }

  def executeQuery(sql: String, objects: Array[Any]): ResultSet = {
    try {
      this.connection = Util.getConnection
      this.preparedStatement = this.connection.prepareStatement(sql)
      if (objects != null)
        this.setPreparedStatement(objects)
      this.resultSet = this.preparedStatement.executeQuery()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      Util.returnConnection(this.connection)
      this.connection == null
    }
    this.resultSet
  }
  def executeInsertOrUpdate(sql: String, objects: Array[Any]): Int = {
    try {
      this.connection = Util.getConnection
      this.preparedStatement = this.connection.prepareStatement(sql)
      if(objects != null)
        this.setPreparedStatement(objects)
      this.preparedStatement.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      Util.returnConnection(this.connection)
      this.connection == null
    }
       -1
  }

  def existsUpdateElseInsert(sqls: List[String], args: List[Array[Any]]): Unit = {
    try { if(sqls.length != 3 || args.length != 3) return
      // 1. 查询
      val rs = this.executeQuery(sqls.head, args.head)
    if(rs.next())
    {
      // 2.1 更新
      if(sqls(1) != null && args(1) != null)
        this.executeInsertOrUpdate(sqls(1), args(1))
    } else {
      // 2.2 插入
      if(sqls(2) != null && args(2) != null)
        this.executeInsertOrUpdate(sqls(2), args(2))
    }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }


  def close() : Unit = {
    try { if(resultSet != null)
      resultSet.close()
    if(preparedStatement != null)
      preparedStatement.close()
      //
      if(connection != null) connection.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }


  }
  object Util {

    // 数据库驱动类
    private val driverClass: String = "com.mysql.jdbc.Driver"
    // 数据库连接地址
    private val url: String = "jdbc:mysql://localhost:3306/testone?useSSL=false&useUnicode=true&characterEncoding=utf8"
    // 数据库连接用户名
    private val username: String = "root"
    // 数据库连接密码
    private val password: String = "123456"

    Class.forName(driverClass)

    val poolSize: Int = 20

    private val pool: BlockingQueue[Connection] = new LinkedBlockingQueue[Connection]()

    for (i <- 1 to poolSize) {
      Util.pool.put(DriverManager.getConnection(url, username, password))
    }

    private def getConnection: Connection = {
      pool.take()
    }

    private def returnConnection(conn: Connection): Unit = {
      Util.pool.put(conn)
    }

    def releaseResource() = {
      val thread = new Thread(new CloseRunnable)
      thread.setDaemon(true)
      thread.start()
    }

    class CloseRunnable extends Runnable {

      override def run(): Unit = {
        while (Util.pool.size > 0) {
          try {
            // println(s"当前连接池大小: ${DBUtil.pool.size}")
            Util.pool.take().close()
          } catch {
            case e: Exception => e.printStackTrace()

          }

        }

      }
    }


    def main(args: Array[String]): Unit = {
      val hello ="hello"
      val sql = "insert into wordcount(word, wordcount) values('"+hello+"'," + 34 +")"
      val u = new Util
      u.execute(sql)
    }

  }