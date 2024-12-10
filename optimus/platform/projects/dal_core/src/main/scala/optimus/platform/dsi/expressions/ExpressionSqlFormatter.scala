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
package optimus.platform.dsi.expressions

import java.util.HashMap
import java.time._
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField

import optimus.dsi.serialization.json.JacksonObjectMapper
import optimus.platform.dsi.expressions.AggregateType._
import optimus.platform.dsi.expressions.BinaryOperator._
import optimus.platform.storable.BusinessEventReference
import optimus.platform.storable.EntityReference
import optimus.platform.storable.SerializedKey
import org.apache.commons.lang3.StringUtils

final case class AbbreviationThreshold(listThreshold: Option[Int], whereLengthThreshold: Option[Int])

class ExpressionSqlFormatter(constFormatter: ConstFormatter, threshold: AbbreviationThreshold)
    extends ExpressionVisitor {
  import ExpressionSqlFormatter._

  private val sb = new StringBuffer()
  private val ids = new HashMap[Id, String]
  private[this] var isNested = false
  private[this] var depth = 0
  private val indent = 2

  protected trait Indentation

  protected object Indentation {
    case object Same extends Indentation
    case object Inner extends Indentation
    case object Outer extends Indentation
  }

  override def toString(): String = sb.toString()

  def write(value: Any): Unit = sb.append(value)

  protected def writeLine(style: Indentation): Unit = {
    sb.append('\n')
    indent(style)
    for (i <- 0 until depth * indent) {
      write(" ")
    }
  }

  protected def indent(style: Indentation): Unit = {
    style match {
      case Indentation.Inner => depth += 1
      case Indentation.Outer => depth -= 1
      case _                 =>
    }
  }

  protected def writeAsAliasName(aliasName: String): Unit = {
    write("AS ")
    writeAliasName(aliasName)
  }

  protected def writeAliasName(aliasName: String): Unit = write(aliasName)

  protected def getAliasName(id: Id): String = {
    var name = ids.get(id)
    if (name == null) {
      name = "A" + id.hashCode + "?"
      ids.put(id, name)
    }
    name
  }

  protected def addIds(e: Expression): Unit = {
    e match {
      case e: Entity =>
        addId(e.id)

      case e: Event =>
        addId(e.id)

      case e: Linkage =>
        addId(e.id)

      case e: Select =>
        addId(e.id)

      case e: Join =>
        addIds(e.left)
        addIds(e.right)

      case e: Embeddable =>
        addId(e.id)

      case _ =>
    }
  }

  protected def addId(id: Id): Unit = {
    if (ids.get(id) eq null) {
      val name = "t" + ids.size
      ids.put(id, name)
    }
  }

  protected override def visitFunction(f: Function): Expression = {
    write(f.method)
    write("(")
    var first = true
    for (arg <- f.arguments) {
      if (!first) write(", ")
      visit(arg)
      first = false
    }
    write(")")
    f
  }

  protected override def visitJoin(j: Join): Expression = {
    visitSource(j.left)
    writeLine(Indentation.Same)
    j.joinType match {
      case JoinType.InnerJoin =>
        write("INNER JOIN ")
      case JoinType.LeftOuter =>
        write("LEFT OUTER JOIN ")
      case JoinType.RightOuter =>
        write("RIGHT OUTER JOIN ")
      case JoinType.FullOuter =>
        write("FULL OUTER JOIN ")
      case JoinType.CrossApply =>
        write("CROSS JOIN LATERAL ")
      case JoinType.CrossJoin =>
        write("CROSS JOIN ")
      case JoinType.OuterApply =>
        write("LEFT OUTER JOIN LATERAL ")
    }
    visitSource(j.right)
    j.on.foreach(on => {
      writeLine(Indentation.Inner)
      write("ON ")
      visitPredicate(on)
      indent(Indentation.Outer)
    })
    j
  }

  protected override def visitMember(m: Member): Expression = {
    throw new IllegalArgumentException(s"The member access '${m.name}' is not supported")
  }

  protected override def visitAggregate(a: Aggregate): Expression = {
    writeAggregateName(a.aggregateType)
    write("(")
    if (a.arguments.isEmpty) {
      if (requiresAsteriskWhenNoArgument(a.aggregateType))
        write("*")
    } else {
      if (a.isDistinct)
        write("DISTINCT ")
      var first = true
      for (arg <- a.arguments) {
        if (!first) write(", ")
        visit(arg)
        first = false
      }
    }
    write(")")
    a
  }

  protected def writeAggregateName(aggregateType: AggregateType): Unit = {
    write(aggregateSql(aggregateType))
  }

  protected def requiresAsteriskWhenNoArgument(aggregateName: AggregateType): Boolean = {
    aggregateName == AggregateType.Count
  }

  protected override def visitSelect(s: Select): Expression = {
    addIds(s.from)
    write("SELECT ")
    if (s.isDistinct)
      write("DISTINCT ")
    writeColumns(s.properties)
    if (s.from ne null) {
      writeLine(Indentation.Same)
      write("FROM ")
      visitSource(s.from)
    }
    s.where.map(where => {
      writeLine(Indentation.Same)
      write("WHERE ")
      val startLength = sb.length()
      visitPredicate(where)
      threshold.whereLengthThreshold.foreach { lengthThreshold =>
        val endLength = sb.length()
        val length = endLength - startLength
        if (length > lengthThreshold) {
          sb.delete(startLength + lengthThreshold - 1, endLength - 1)
          write(" ... ")
        }
      }
    })
    if ((s.groupBy ne null) && s.groupBy.nonEmpty) {
      writeLine(Indentation.Same)
      write("GROUP BY ")
      var first = true
      for (g <- s.groupBy) {
        if (!first) write(", ")
        visit(g)
        first = false
      }
    }
    if ((s.sortBy ne null) && s.sortBy.nonEmpty) {
      writeLine(Indentation.Same)
      write("ORDER BY ")
      var first = true
      for (o <- s.sortBy) {
        if (!first) write(", ")
        visit(o.e)
        if (o.sortType == SortType.Desc) {
          write(" DESC NULLS LAST")
        } else write(" NULLS FIRST")
        first = false
      }
    }
    s.take.map(take => {
      writeLine(Indentation.Same)
      write("LIMIT ")
      visit(take)

      s.skip.map(skip => {
        writeLine(Indentation.Same)
        write("OFFSET ")
        visit(skip)
      })
    })
    s
  }

  protected def writeColumns(columns: List[PropertyDef]): Unit = {
    if (columns.size > 0) {
      for ((col, i) <- columns.zipWithIndex) {
        if (i > 0) write(", ")
        val c = visit(col.e)
        if (StringUtils.isNotEmpty(col.name) && columnNameNotMatch(c, col.name)) {
          write(" ")
          writeAsColumnName(col.name)
        }
      }
    } else {
      write("NULL ")
      if (isNested) {
        writeAsColumnName("tmp")
        write(" ")
      }
    }
  }

  protected def writeAsColumnName(columnName: String): Unit = {
    write("AS ")
    writeColumnName(columnName)
  }

  protected def writeColumnName(columnName: String): Unit = {
    val shouldQuote = columnName.contains(".")
    if (shouldQuote)
      write('"')
    write(columnName)
    if (shouldQuote)
      write('"')
  }

  private def columnNameNotMatch(e: Expression, columnName: String): Boolean = {
    e match {
      case p: Property if p.names.size == 1 => p.names.head != columnName
      case _                                => true
    }
  }

  protected def visitSource(source: Expression): Expression = {
    val saveIsNested = isNested
    isNested = true
    source match {
      case e: Entity     => writeEntitySource(e)
      case e: Event      => writeEventSource(e)
      case e: Linkage    => writeLinkageSource(e)
      case e: Embeddable => writeEmbeddableSource(e)
      case e: Select =>
        write("( ")
        writeLine(Indentation.Inner)
        visit(e)
        writeLine(Indentation.Same)
        write(") ")
        writeAsAliasName(getAliasName(e.id))
        indent(Indentation.Outer)
      case e: Join =>
        visitJoin(e)
      case _ => visit(source)
    }
    isNested = saveIsNested
    source
  }

  protected def writeEntitySource(e: Entity): Unit = {
    write(e.name)
    write(" ")
    writeAsAliasName(getAliasName(e.id))
  }

  protected def writeEventSource(e: Event): Unit = {
    write(e.name)
    write(" ")
    writeAsAliasName(getAliasName(e.id))
  }

  protected def writeLinkageSource(e: Linkage): Unit = {
    write(s"Linkage(${e.name})")
    write(" ")
    writeAsAliasName(getAliasName(e.id))
  }

  protected def writeEmbeddableSource(e: Embeddable): Unit = {
    write(s"${e.entity}_${e.property}_${e.tag}")
    write(" ")
    writeAsAliasName(getAliasName(e.id))
  }

  protected override def visitScalar(s: Scalar): Expression = {
    write("(")
    writeLine(Indentation.Inner)
    visit(s.query)
    writeLine(Indentation.Same)
    write(")")
    indent(Indentation.Outer)
    s
  }

  protected override def visitExists(e: Exists): Expression = {
    write("EXISTS (")
    writeLine(Indentation.Inner)
    visit(e.query)
    writeLine(Indentation.Same)
    write(")")
    indent(Indentation.Outer)
    e
  }

  protected override def visitIn(i: In): Expression = {
    visit(i.e)
    write(" IN (")
    i.values match {
      case Left(s) =>
        write("(")
        writeLine(Indentation.Inner)
        visit(s)
        writeLine(Indentation.Same)
        write(")")
        indent(Indentation.Outer)
      case Right(l) =>
        val size = l.size
        threshold.listThreshold.filter(th => size > th && size > 2) map { _ =>
          visit(l.head)
          write(s"... [${size - 2} elements]... ")
          visit(l.last)
        } getOrElse {
          var first = true
          for (v <- l) {
            if (!first) write(", ")
            visit(v)
            first = false
          }
        }
    }

    write(")")
    i
  }

  protected override def visitBinary(b: Binary): Expression = {
    val opString = operatorSql(b.op)
    val left = b.left
    val right = b.right

    def writeCommon(left: Expression, right: Expression): Unit = {
      visit(left)
      write(" ")
      write(opString)
      write(" ")
      visit(right)
    }

    write("(")
    b.op match {
      case AndAlso | OrElse =>
        visitPredicate(left)
        write(" ")
        write(opString)
        write(" ")
        visitPredicate(right)

      case Equal =>
        (left, right) match {
          case (Constant(null | None, _), _) =>
            visit(right)
            write(" IS NULL")
          case (_, Constant(null | None, _)) =>
            visit(left)
            write(" IS NULL")
          case (e: Exists, Constant(false, _)) =>
            write("NOT ")
            visit(e)
          case _ => writeCommon(left, right)
        }

      case NotEqual =>
        (left, right) match {
          case (Constant(null | None, _), _) =>
            visit(right)
            write(" IS NOT NULL")
          case (_, Constant(null | None, _)) =>
            visit(left)
            write(" IS NOT NULL")
          case _ => writeCommon(left, right)
        }

      case LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual | Add | Subtract | Multiply | Divide |
          Modulo =>
        writeCommon(left, right)
    }
    write(")")
    b
  }

  // def formatConstant is also used in HashLikeBtreeIndexOps.Equals. The implementation should not be changed by subclass.
  final protected override def visitConstant(c: Constant): Expression = {
    val v = formatConstant(c)
    write(v)
    c
  }

  protected override def visitProperty(p: Property): Expression = {
    if ((p.owner ne Id.EmptyId)) {
      writeAliasName(getAliasName(p.owner))
      write(".")
    }
    writeColumnName(p.names.head)
    p
  }

  def visitPredicate(predicate: Expression): Expression = {
    visit(predicate)
    if (!isPredicate(predicate))
      write(" = TRUE")
    predicate
  }

  protected def isPredicate(element: Expression): Boolean = {
    element match {
      case Binary(op, _, _) =>
        op match {
          case Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual | AndAlso | OrElse =>
            true
          case _ => false
        }
      case e: In                                         => true
      case e: Constant if e.typeCode == TypeCode.Boolean => true
      case f: Function                                   => true
      case c: Condition                                  => true
      case e: Exists                                     => true
      case _                                             => false
    }
  }

  override protected def visitCondition(c: Condition): Expression = {
    write("CASE WHEN ")
    visit(c.check)
    write(" THEN ")
    visit(c.ifTrue)
    write(" ELSE ")
    visit(c.ifFalse)
    write(" END")
    c
  }

  def formatConstant(c: Constant): String = {
    constFormatter.format(c.value)
  }

  def getSqlType(e: Expression): String = ""
}

object ExpressionSqlFormatter {
  val MaxThreshold = AbbreviationThreshold(None, None)

  def format(e: Expression, constFormatter: ConstFormatter, threshold: AbbreviationThreshold = MaxThreshold): String = {
    val formatter = new ExpressionSqlFormatter(constFormatter, threshold)
    formatter.visit(OptionValueEvaluator.visit(e))
    formatter.toString
  }

  // Get sql according to AggregateType.
  val aggregateSql: PartialFunction[AggregateType, String] = {
    case Sum         => "sum"
    case Count       => "count"
    case Average     => "avg"
    case Min         => "min"
    case Max         => "max"
    case ArrayAgg    => "array_agg"
    case Var_samp    => "var_samp"
    case Var_pop     => "var_pop"
    case Stddev_samp => "stddev_samp"
    case Stddev_pop  => "stddev_pop"
    case Corr        => "corr"
    case Distinct    => throw new UnsupportedOperationException(s"Unexpectced AggregateType: Distinct")
  }

  // Get sql according to BinaryOperator.
  val operatorSql: PartialFunction[BinaryOperator, String] = {
    case Equal              => "="
    case NotEqual           => "!="
    case LessThan           => "<"
    case LessThanOrEqual    => "<="
    case GreaterThan        => ">"
    case GreaterThanOrEqual => ">="
    case AndAlso            => "AND"
    case OrElse             => "OR"
    case Add                => "+"
    case Subtract           => "-"
    case Multiply           => "*"
    case Divide             => "/"
    case Modulo             => "%"
  }

  val timestamptzFmt = new DateTimeFormatterBuilder()
    .appendPattern("yyyy-MM-dd HH:mm:ss")
    .appendFraction(ChronoField.NANO_OF_SECOND, 3, 9, true)
    .appendPattern(" Z")
    .toFormatter
  val timetzFmt = new DateTimeFormatterBuilder()
    .appendPattern("HH:mm:ss")
    .appendFraction(ChronoField.NANO_OF_SECOND, 3, 9, true)
    .appendPattern(" Z")
    .toFormatter
  val dateFmt = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd").toFormatter
  val timeFmt = new DateTimeFormatterBuilder()
    .appendPattern("HH:mm:ss")
    .appendFraction(ChronoField.NANO_OF_SECOND, 3, 9, true)
    .toFormatter
}

trait ConstFormatter {
  def format(value: Any): String
}

class DefaultConstFormatter extends ConstFormatter {
  protected val doubleQuotes = "\""

  def format(value: Any): String = {
    value match {
      case null                                 => "NULL"
      case str: String                          => s"'${str.replace("'", "''")}'"
      case v: Double if v.isNaN || v.isInfinity => s"'$v'"
      case v: Float if v.isNaN || v.isInfinity  => s"'$v'"
      case _ =>
        val res = formatInner(value)
        value match {
          case _ @(_: Int | _: Short | _: Long | _: Double | _: Boolean | _: Byte | _: Char) => res
          case _ =>
            val str = res.replace("'", "''")
            s"'$str'"
        }
    }
  }

  protected def formatInner(value: Any): String = {
    def serialize(v: Any): Any = ??? /* {
      v match {
        case e: EntityReference         => e.toString
        case be: BusinessEventReference => be.toString
        case ch: Char                   => ch.toInt
        case i: ZoneId                  => i.getId
        case t: ZonedDateTime           => ExpressionSqlFormatter.timestamptzFmt.format(t)
        case t: LocalDate               => ExpressionSqlFormatter.dateFmt.format(t)
        case t: LocalTime               => ExpressionSqlFormatter.timeFmt.format(t)
        case t: OffsetTime              => ExpressionSqlFormatter.timetzFmt.format(t)
        case sk: SerializedKey =>
          s"SK(${sk.properties.map { case (k, v) => s"$k=${serialize(v)}" }.mkString(",")})"
        case seq: Seq[_] => // Seq/ Tuple.
          seq.map(serialize(_))

        case m: Map[_, _] => // case class.
          m.map { case (key, value) =>
            (serialize(key), serialize(value))
          }

        case _ => v
      }
    } */

    serialize(value) match {
      case str: String => str
      case v           => JacksonObjectMapper.mapper.writeValueAsString(v)
    }
  }
}
