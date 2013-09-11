package isucon2

import java.sql.{DriverManager,ResultSet,Statement}
import java.text.SimpleDateFormat
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import org.jboss.netty.handler.codec.http.HttpHeaders
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import akka.actor.{Actor, ActorRef}
import xitrum.{ Action, SkipCSRFCheck, Server }
import xitrum.annotation.{ GET, POST }
import xitrum.util.Loader
import akka.dispatch.Foreach
import xitrum.SkipCSRFCheck

object Boot {
  def main(args: Array[String]) {
    Class.forName("com.mysql.jdbc.Driver").newInstance()

    Server.start()
  }
}

object RecentSold {

  def get():ArrayBuffer[(String, String, String, String)] = {
    val items = new ArrayBuffer[(String, String, String, String)]
        val sql = """SELECT stock.seat_id, variation.name AS v_name, ticket.name AS t_name, artist.name AS a_name FROM stock
           JOIN variation ON stock.variation_id = variation.id
           JOIN ticket ON variation.ticket_id = ticket.id
           JOIN artist ON ticket.artist_id = artist.id
         WHERE order_id IS NOT NULL
         ORDER BY order_id DESC LIMIT 10"""
      val con = DriverManager.getConnection("jdbc:mysql://localhost/isucon2", "root", "root")
      val stmt = con.createStatement();
      val rs = stmt.executeQuery(sql)
      while (rs.next()) {
        val id = rs.getString("seat_id")
        val v_name = rs.getString("v_name")
        val t_name = rs.getString("t_name")
        val a_name = rs.getString("a_name")
        val item = (id, v_name, t_name, a_name)
        items.append(item)
      }

      rs.close();
      stmt.close();
      con.close();
      items
  }

}

trait DefaultLayout extends Action with SkipCSRFCheck{
  override def layout = renderViewNoLayout(classOf[DefaultLayout])

  protected def executeQuery(sql: String)(fun: (ResultSet) => Unit) {
    val con = DriverManager.getConnection("jdbc:mysql://localhost/isucon2", "root", "root")
    val stmt = con.createStatement();
    val rs = stmt.executeQuery(sql)
    fun(rs)
    rs.close();
    stmt.close();
    con.close();
  }
  at("recent_sold") = RecentSold.get()

}

@GET("")
class SiteIndex extends DefaultLayout {
  def execute() {
    executeQuery("SELECT * FROM artist ORDER BY id") { rs =>
      val artists = new ArrayBuffer[(Int, String)]
      while (rs.next()) {
        val id = rs.getInt("id")
        val name = rs.getString("name")
        val artist = (id, name)
        artists.append(artist)
      }

      at("artists") = artists
      respondView()
    }
  }
}

@GET("artist/:artistid")
class ArtistShow extends DefaultLayout {
  def execute() {
    val artistid = param("artistid")

    executeQuery(s"SELECT id, name FROM artist WHERE id = ${artistid} LIMIT 1") { rs =>

      if (rs.next()) {
        val id = rs.getInt("id")
        val name = rs.getString("name")
        val artist = (id, name)
        at("artist") = artist

        executeQuery(s"SELECT id, name FROM ticket WHERE artist_id = ${id} ORDER BY id") { rs2 =>
          val tickets = new ArrayBuffer[(Int, String, String)]
          while (rs2.next()) {
            val tid = rs2.getInt("id")
            val tname = rs2.getString("name")
            val countsql = s"""SELECT COUNT(*) AS cnt FROM variation
                               INNER JOIN stock ON stock.variation_id = variation.id
                               WHERE variation.ticket_id = ${tid} AND stock.order_id IS NULL"""

            executeQuery(countsql) { rs3 =>
              if (rs3.next()) {
                val ticket = (tid, tname, rs3.getString("cnt"))
                tickets.append(ticket)
              }
            }
          }
          at("tickets") = tickets
          respondView()
        }
      }
    }
  }
}

@GET("ticket/:ticketid")
class TicketShow extends DefaultLayout {
  def execute() {

    val ticketid = param("ticketid")

    val sql = s"""SELECT t.*, a.name AS artist_name FROM ticket t
                  INNER JOIN artist a ON t.artist_id = a.id
                  WHERE t.id = ${ticketid} LIMIT 1"""

    executeQuery(sql) { rs =>
      if (rs.next()) {
        val id = rs.getInt("id")
        val name = rs.getString("name")
        val artist_id = rs.getInt("artist_id")
        val artist_name = rs.getString("artist_name")
        val ticket = (id, name, artist_id, artist_name)
        at("ticket") = ticket
        executeQuery(s"SELECT id, name FROM variation WHERE ticket_id = ${id} ORDER BY id") { rs2 =>
          val variations = new ArrayBuffer[(Int, String, Int, HashMap[String, Int])]
          while (rs2.next()) {
            val vid = rs2.getInt("id")
            val vname = rs2.getString("name")
            val countsql = s"""SELECT COUNT(*) AS cnt FROM stock
                               WHERE variation_id = ${vid} AND order_id IS NULL"""
            executeQuery(countsql) { rs3 =>
              if (rs3.next()) {
                val count = rs3.getInt("cnt")
                val stocksql = s"""SELECT seat_id, order_id FROM stock
                                   WHERE variation_id = ${vid}"""
                executeQuery(stocksql) { rs4 =>
                  val stock = new HashMap[String, Int]
                  while (rs4.next()) {
                    stock.put(rs4.getString("seat_id"), rs4.getInt("order_id"))
                  }
                  variations.append((vid, vname, count, stock))
                }
              }
            }
          }
          at("variations") = variations
        }
      }
    }
    respondView()
  }
}

@POST("buy")
class TicketBuy extends DefaultLayout {

  def execute() {
    val memberId = param("member_id")
    val variationId = param("variation_id")
    val con = DriverManager.getConnection("jdbc:mysql://localhost/isucon2", "root", "root")
    val stmt = con.createStatement();

    stmt.executeQuery("BEGIN")
    stmt.execute(s"INSERT INTO order_request (member_id) VALUES ('${memberId}')", Statement.RETURN_GENERATED_KEYS)
    val rs = stmt.getGeneratedKeys();
    if (rs.next()) {
      val lastId = rs.getInt(1);
      println(lastId)
      val updSql = s"""UPDATE stock SET order_id = ${lastId}
                      WHERE variation_id = ${variationId} AND order_id IS NULL
                      ORDER BY RAND() LIMIT 1"""
      val affectedRows = stmt.executeUpdate(updSql)
      if (affectedRows > 0) {
        val sql = s"SELECT seat_id FROM stock WHERE order_id =${lastId} LIMIT 1"
        val rs2 = stmt.executeQuery(sql)
        if (rs2.next()) {
          val seatId = rs2.getString("seat_id")
          stmt.executeQuery("COMMIT")
          rs.close()
          stmt.close()
          con.close()
          at("seatId") = seatId
          at("memberId") = memberId
          respondView()
        }
      } else {
          stmt.executeQuery("ROLLBACK")
          rs.close()
          stmt.close()
          con.close();
          respondText("soldOut")
      }
    }
  }
}

@GET("/admin")
class Admin extends DefaultLayout {
  def execute() {
      respondView()
  }
}

@GET("/admin/order")
class CSV extends Action {
  def execute() {
    val items = new ArrayBuffer[(Int, String, String, String)]
    val sql = """SELECT order_request.*, stock.seat_id, stock.variation_id, stock.updated_at
             FROM order_request JOIN stock ON order_request.id = stock.order_id
             ORDER BY order_request.id ASC"""
    val con = DriverManager.getConnection("jdbc:mysql://localhost/isucon2", "root", "root")
    val stmt = con.createStatement();
    val rs = stmt.executeQuery(sql)
    var body =""
    while (rs.next()) {
      val id = rs.getInt("id")
      val member_id = rs.getString("member_id")
      val seat_id = rs.getInt("seat_id")
      val variation_id = rs.getInt("variation_id")
      val updated_at = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(rs.getDate("updated_at"))
      body += s"""${id},${member_id},${seat_id},${variation_id},${variation_id},${updated_at}
"""
    }
    response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/csv")
    response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, body.getBytes(CharsetUtil.UTF_8).length)
    respondText(body)
  }
}

@POST("/admin")
class Init extends Action with SkipCSRFCheck{
  def execute() {
    val con = DriverManager.getConnection("jdbc:mysql://localhost/isucon2", "root", "root")
    val sql = Loader.stringFromClasspath("initial_data.sql")
    sql.split(";").foreach{s =>
    if (s.length > 1){
        val stmt = con.createStatement()
        stmt.execute(s)
        stmt.close()
      }
    }
    con.close();
    redirectTo("/admin", HttpResponseStatus.FOUND)
  }
}