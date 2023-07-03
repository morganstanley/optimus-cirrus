/*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package optimus.platform.relational.formatter

import java.io._
import java.time._
import optimus.platform._
import optimus.platform.AsyncImplicits._
import optimus.platform.relational._
import optimus.platform.relational.tree._

import scala.collection.immutable

trait CellFormatter[-T] { def format(cell: T): String }

final case class SignificantFigures(n: Int) extends CellFormatter[Double] {
  def format(cell: Double): String = {
    val multiplier = math.pow(10, (n - (math.log(cell) / math.log(10)).round - 1).toDouble)
    ((cell * multiplier).round.toDouble / multiplier).toString
  }

  override def toString = n + " s.f."
}

final case class DecimalPlaces(n: Int) extends CellFormatter[Double] {
  def format(cell: Double): String = {
    val multiplier = math.pow(10, n)
    ((cell * multiplier).round.toDouble / multiplier).toString
  }

  override def toString = n + " d.p."
}

object OutputSettings {
  val basicCellFormatter = new CellFormatter[Any] { def format(cell: Any) = cell.toString }
  implicit val defaultOutputSettings: OutputSettings = OutputSettings(Map())
}

final case class OutputSettings(cellFormatters: Map[String, CellFormatter[_]], reserveListMapKeysOrder: Boolean = false)

abstract class Formatter {
  type OutputFormat
  def defaultOutput: OutputFormat

  trait Tabulator[-T] {
    @async def tabulate(t: T)(implicit timeZone: Option[ZoneId], settings: OutputSettings): Tabulation
  }

  protected case class Tabulation(columns: Seq[String], headers: Iterable[Seq[String]], points: Iterable[Seq[String]]) {
    def isEmpty = points.isEmpty && headers.isEmpty
  }

  protected def escape(s: String): String
  protected def widths(rows: Iterable[Seq[String]], names: Seq[String]): Seq[Int]

  protected def tabulator[T: Tabulator] = implicitly[Tabulator[T]]
  protected object Tabulator extends Tiers

  protected def tabulateStringMap[Row](
      rs: Iterable[Map[String, Row]]
  )(implicit timeZone: Option[ZoneId], settings: OutputSettings): Tabulation
  protected def joinTabulations(left: Tabulation, right: Tabulation): Tabulation

  class Tiers extends MapHandler with GroupByQueryHandler with QueryProxyHandler

  // ----------------------------------------------------------------------------------------------------------------------
  // ---------------------------------------- Query Output handler             --------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------------
  trait GroupByQueryHandler extends QueryHandler {
    this: Tiers =>
    // Execute a group-by query, and recurse on the tabulator of the result type
    implicit def groupByQueryTabulator[T, U](implicit tab: Tabulator[Iterable[T]]): Tabulator[GroupQuery[U, Query[T]]] =
      new Tabulator[GroupQuery[U, Query[T]]] {
        @async def tabulate(
            query: GroupQuery[U, Query[T]]
        )(implicit timeZone: Option[ZoneId], settings: OutputSettings): Tabulation = {
          val groupByResults = Query.execute(query)
          val results = if (groupByResults.nonEmpty) groupByResults.apar.map { case (k, v) => (k, tab.tabulate(v)) }
          else Seq("<empty relational>" -> tab.tabulate(Iterable.empty))
          val right = results.map(_._2)
          joinTabulations(
            Tabulation(List("key"), Seq(Seq("key")), results.map(k => Seq(k._1.toString))),
            Tabulation(
              right.head.headers.head,
              right.head.headers,
              right map (_.points.toIndexedSeq.transpose map (_ mkString " "))
            )
          )
        }
      }
  }

  /**
   * Match an implicit on the basis of its `T` type being a subtype of (ShapeLeft, Query[SubVal]). Manually inspect the
   * typeInfo at runtime to determine how to evaluate the Query.
   */
  trait QueryHandler extends QueryBaseHandler {
    this: Tiers =>
    implicit def queryTabulator[T <: (_, _): TypeInfo](implicit tab: Tabulator[Iterable[T]]): Tabulator[Query[T]] =
      new Tabulator[Query[T]] {
        @async def tabulate(
            query: Query[T]
        )(implicit timeZone: Option[ZoneId], settings: OutputSettings): Tabulation = {
          val results = Query.execute(query)
          val QueryClass = classOf[Query[_]]
          typeInfo[T] match {
            case TupleTypeInfo(_, TypeInfo(QueryClass +: _, _, _, Seq(rShape))) =>
              val rs =
                if (results.nonEmpty)
                  results
                    .asInstanceOf[Iterable[(_, Query[Any])]]
                    .apar(PriqlSettings.concurrencyLevel)
                    .map { case (k, v) => (k, Query.execute(v)) }
                else Seq("<empty relational>" -> Iterable.empty)
              val right = rs.apar map { case (k, v) =>
                (k.toString, staticTabulator[Any](rShape.asInstanceOf[TypeInfo[Any]]).tabulate(v))
              } map (_._2)
              joinTabulations(
                Tabulation(List("key"), Seq(Seq("key")), rs.map(k => Seq(k._1.toString))),
                Tabulation(
                  right.head.headers.head,
                  right.head.headers,
                  right map (_.points.toIndexedSeq.transpose map (_ mkString " "))
                )
              )
            case _ =>
              tab.tabulate(results)
          }
        }
      }
  }

  trait QueryBaseHandler {
    this: Tiers =>
    // Execute the query, and recurse on the result, an Iterable[T]
    implicit def queryBaseTabulator[T](implicit tab: Tabulator[Iterable[T]]): Tabulator[Query[T]] =
      new Tabulator[Query[T]] {
        @async def tabulate(
            query: Query[T]
        )(implicit timeZone: Option[ZoneId], settings: OutputSettings): Tabulation = {
          tab.tabulate(Query.execute(query))
        }
      }
  }

  trait QueryProxyHandler {
    this: Tiers =>
    // Execute the query in QueryProxy, and recurse on the result, an Iterable[T]
    implicit def queryProxyTabulator[T, K <: RelationKey[T]](implicit
        tab: Tabulator[Iterable[T]]
    ): Tabulator[QueryProxy[T, K]] = new Tabulator[QueryProxy[T, K]] {
      @async def tabulate(
          proxy: QueryProxy[T, K]
      )(implicit timeZone: Option[ZoneId], settings: OutputSettings): Tabulation = {
        val results = Query.execute(proxy.query)
        val QueryClass = classOf[Query[_]]
        proxy.shapeTypeTag match {
          case TupleTypeInfo(_, TypeInfo(QueryClass +: _, _, _, Seq(rShape))) =>
            val rs =
              if (results.nonEmpty)
                results
                  .asInstanceOf[Iterable[(_, Query[Any])]]
                  .apar(PriqlSettings.concurrencyLevel)
                  .map { case (k, v) => (k, Query.execute(v)) }
              else Seq("<empty relational>" -> Iterable.empty)
            val right = rs.apar map { case (k, v) =>
              (k.toString, staticTabulator[Any](rShape.asInstanceOf[TypeInfo[Any]]).tabulate(v))
            } map (_._2)
            joinTabulations(
              Tabulation(List("key"), Seq(Seq("key")), rs.map(k => Seq(k._1.toString))),
              Tabulation(
                right.head.headers.head,
                right.head.headers,
                right map (_.points.toIndexedSeq.transpose map (_ mkString " "))
              )
            )
          case _ =>
            tab.tabulate(results)
        }
      }
    }
  }

  trait MapHandler extends TupleHandler {
    this: Tiers =>
    // Tabulate Maps, where the map keys form the column headings, and the values the rows
    implicit def mapTabulator[Row]: Tabulator[Iterable[Map[String, Row]]] = new Tabulator[Iterable[Map[String, Row]]] {
      @async def tabulate(
          rs: Iterable[Map[String, Row]]
      )(implicit timeZone: Option[ZoneId], settings: OutputSettings): Tabulation =
        tabulateStringMap(rs)
    }
  }

  trait TupleHandler extends DynamicObjectHandler {
    this: Tiers =>
    // Recurse on both sides of the tuple, and join both sides together
    implicit def tupleTabulator[L, R](implicit
        lTab: Tabulator[Iterable[L]],
        rTab: Tabulator[Iterable[R]]
    ): Tabulator[Iterable[(L, R)]] = new Tabulator[Iterable[(L, R)]] {
      @async def tabulate(
          rs: Iterable[(L, R)]
      )(implicit timeZone: Option[ZoneId], settings: OutputSettings): Tabulation = {
        val left: Tabulation = lTab.tabulate(rs.map(_._1))
        val right: Tabulation = rTab.tabulate(rs.map(_._2))
        if (left.isEmpty) right
        else if (right.isEmpty) left
        else joinTabulations(left, right)
      }
    }
  }

  trait DynamicObjectHandler extends InlineHandler {
    this: Tiers =>
    // Tabulate DynamicObjects, by recursing on the map produced by the DynamicObject#getAll method
    implicit def dynamicObjectTabulator[A <: DynamicObject]: Tabulator[Iterable[A]] = new Tabulator[Iterable[A]] {
      @async def tabulate(rs: Iterable[A])(implicit timeZone: Option[ZoneId], settings: OutputSettings): Tabulation =
        mapTabulator.tabulate(rs.map(_.getAll))
    }
  }

  trait InlineHandler extends StaticHandler {
    this: Tiers =>
    // Inline rows
    implicit def inlineTabulator[Row: CellFormatter]: Tabulator[Iterable[Row]] = new Tabulator[Iterable[Row]] {
      @async def tabulate(
          rows: Iterable[Row]
      )(implicit timeZone: Option[ZoneId], settings: OutputSettings): Tabulation = {
        val rowLengths = rows.map(r => implicitly[CellFormatter[Row]].format(r).length).toSeq
        val max = (0 +: rowLengths).max + 1 // (add 0 to avoid empty seq errors)
        if (max == 0) Tabulation(Nil, Nil, Nil)
        else
          Tabulation(
            List(""),
            Seq(Seq(escape(""))),
            rows map { r => Seq(escape(implicitly[CellFormatter[Row]].format(r))) }
          )
      }
    }
  }

  trait StaticHandler extends StringHandler {
    this: Tiers =>
    // Tabulate Rows where we statically know the type of Row, and can convert it into columns
    implicit def staticTabulator[Row: TypeInfo]: Tabulator[Iterable[Row]] = new Tabulator[Iterable[Row]] {
      @async def tabulate(
          rs: Iterable[Row]
      )(implicit timeZone: Option[ZoneId], settings: OutputSettings): Tabulation = {
        val names = typeInfo[Row].propertyNames
        mapTabulator[String].tabulate(
          if (rs.isEmpty) Seq((names.headOption.map(_ -> "<empty relational>") ++ names.drop(1).map(_ -> "")).toMap)
          else
            rs map { r =>
              names.iterator.map { n =>
                n -> formatValue(n, typeInfo[Row].propertyValue(n, r), settings)
              }.toMap
            }
        )
      }
    }
  }

  trait StringHandler {
    this: Tiers =>
    // Tabulate rows where we know nothing about the row type, so just call toString on each one.
    implicit def toStringTabulator[Row]: Tabulator[Iterable[Row]] =
      inlineTabulator[Row](new CellFormatter[Row] { def format(cell: Row) = cell.toString })
  }

  protected def formatValue[V](n: String, v: V, settings: OutputSettings)(implicit timeZone: Option[ZoneId]): String =
    v match {
      case zdt: ZonedDateTime =>
        val formatter = settings.cellFormatters.get(n).getOrElse(null)
        timeZone
          .map(tz => {
            val ldt = zdt.withZoneSameInstant(tz).toOffsetDateTime.toLocalDateTime
            if (formatter != null) formatter.asInstanceOf[CellFormatter[LocalDateTime]].format(ldt)
            else ldt.toString.replace("T", " ")
          })
          .getOrElse {
            if (formatter != null) formatter.asInstanceOf[CellFormatter[ZonedDateTime]].format(zdt) else zdt.toString
          }
      case null          => "null"
      case other: String => other
      case other =>
        val formatter =
          settings.cellFormatters.get(n).getOrElse(OutputSettings.basicCellFormatter).asInstanceOf[CellFormatter[Any]]
        formatter.format(other)
    }

  private final val GetMatcher = "^get([A-Z0-9])(.*)$".r
  protected def formatHeader(s: String) = s match {
    case GetMatcher(h, t) => h.toLowerCase + t
    case _                => s
  }
}
object Formatter {
  val emptyRelationalString = "<empty>"
}

abstract class StringFormatter(
    protected val joinSeparator: String,
    protected val columnSeparator: String,
    protected val printRule: Boolean,
    protected val pad: Boolean
) extends Formatter {
  type OutputFormat = OutputStream
  def defaultOutput = System.out
  private def rule(s: String) = s map { case '|' => '|'; case _ => '-' }

  @async def format[RowSeq: Tabulator](data: RowSeq, os: OutputFormat = defaultOutput, timeZone: Option[ZoneId] = None)(
      implicit settings: OutputSettings
  ): Unit = {
    implicit val tz = timeZone
    val tab = tabulator[RowSeq].tabulate(data)
    format(tab, os, timeZone)
  }

  @async def format(tab: Tabulation, os: OutputFormat, timeZone: Option[ZoneId]): Unit = {
    val out = new PrintWriter(os)
    if (!tab.points.isEmpty) {
      val zeros = Seq.fill(tab.points.head.length)(0)
      val widths: Seq[Int] = if (pad) (tab.headers ++ tab.points).foldLeft(zeros) { case (a, n) =>
        a zip n map { case (c1, c2) => c1 max c2.length }
      }
      else zeros
      tab.headers filterNot (_ forall (_ matches "[\\s|\\|]*")) foreach { hs =>
        val head = (hs zip widths map { case (h, w) => h + " " * (w - h.length) }) mkString columnSeparator
        out.println(head)
        if (printRule) out.println(rule(head))
      }
      tab.points map (ps =>
        ps zip widths map { case (p, w) => p + " " * (w - p.length) } mkString columnSeparator) foreach out.println
      out.flush()
    }
  }

  def tabulateStringMap[Row](
      rs: Iterable[Map[String, Row]]
  )(implicit timeZone: Option[ZoneId], settings: OutputSettings): Tabulation = {
    val names = rs.headOption match {
      case None                                                                        => Seq.empty[String]
      case Some(m: immutable.ListMap[String, Row]) if settings.reserveListMapKeysOrder => m.keys.toIndexedSeq
      case Some(m)                                                                     => m.keys.toIndexedSeq.sorted
    }
    val rows = rs map { r =>
      names map { n =>
        val value = r(n)
        formatValue(n, value, settings)
      }
    }
    Tabulation(names, Seq(names map formatHeader map escape), rows map (_ map escape))
  }

  @async def tabulateTuples[L, R](
      rs: Iterable[(L, R)]
  )(implicit timeZone: Option[ZoneId], lTab: Tabulator[Iterable[L]], rTab: Tabulator[Iterable[R]]): Tabulation = {
    val left: Tabulation = lTab.tabulate(rs.map(_._1))
    val right: Tabulation = rTab.tabulate(rs.map(_._2))
    if (left.isEmpty) right
    else if (right.isEmpty) left
    else joinTabulations(left, right)
  }

  def joinTabulations(left: Tabulation, right: Tabulation): Tabulation = {
    def join(l: Seq[String], r: Seq[String]): Seq[String] = l ++ Seq(joinSeparator) ++ r

    Tabulation(
      left.headers.head ++ right.headers.head,
      left.headers zip right.headers map (join _).tupled,
      left.points zip right.points map (join _).tupled
    )
  }

  def tabulateIterable(headers: Seq[String], rows: Iterable[Seq[String]]): Tabulation =
    Tabulation(headers, Seq(headers map formatHeader map escape), rows map (_ map escape))
}

object Csv extends StringFormatter(",", ",", false, pad = false) {
  def escape(s: String) = if (s matches "-?[0-9]*(\\.[0-9]*)?") s else "\"" + s.replaceAll("\\\"", "\\\"\\\"") + "\""
  def widths(rows: Iterable[Seq[String]], names: Seq[String]) = Seq.fill(names.size)(0)
}

object TableDisplay extends StringFormatter("| ", "  ", true, pad = true) {
  def escape(s: String) = s
  def widths(rows: Iterable[Seq[String]], names: Seq[String]) = rows.foldLeft(names.map(_.length))(_ zip _ map {
    case (a, b) => a max b.length
  }) map (_ + 1)
}
