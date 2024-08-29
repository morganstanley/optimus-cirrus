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

import optimus.graph.DiagnosticSettings
import optimus.platform.dsi.bitemporal.QueryResultMetaData._
import optimus.platform.storable.Key
import optimus.platform.storable.SerializedKey

import java.util.HashMap
import scala.collection.mutable

class ExpressionPriQLFormatter protected (listAbbreviationThreshold: Int) extends ExpressionVisitor {
  import ExpressionPriQLFormatter._

  private val sb = new StringBuffer()
  private val ids = new HashMap[Id, String]

  override def toString(): String = sb.toString()

  protected def formatTake(take: Option[Expression]): Unit = {
    take.foreach(_ match {
      case Constant(num: Int, _) =>
        write(s".takeFirst($num)")
      case x =>
        throw new UnsupportedOperationException(s"Unsupported server side take: $x")
    })
  }

  protected override def visitSelect(s: Select): Expression = {
    addIds(s.from)
    visitSource(s.from)

    s.where.foreach { w =>
      write(".filter(")
      writeParameterList(s.from)
      write(" => ")
      visit(w)
      write(")")
    }

    s.sortBy match {
      case Nil =>
      case SortByDef(d, Property(_, Seq(StorageTxTime), _)) :: Nil =>
        if (d == SortType.Asc) write(".sortByTT") else write(".sortByTTDesc")
      case x =>
        throw new UnsupportedOperationException(s"Unsupported server side sort: $x")
    }
    formatTake(s.take)

    s.properties match {
      case Nil                                                                                              =>
      case PropertyDef(_, Function(ConvertOps.ToEntity.Name, Property(_, Seq(EntityRef), _) :: Nil)) :: Nil =>
      case PropertyDef(_, Property(_, Seq(EntityRef), _)) :: PropertyDef(
            _,
            Property(_, Seq(StorageTxTime), _)) :: Nil =>
        write(".executeReference")
      case PropertyDef(_, Property(_, Seq(EntityRef), _)) :: PropertyDef(_, Property(_, Seq(StorageTxTime), _)) ::
          PropertyDef(_, Property(_, Seq(VersionedRef), _)) :: Nil =>
        write(".executeReference")
      case PropertyDef(_, Aggregate(AggregateType.Count, Nil, false)) :: Nil =>
        write(".count")
      case _ =>
        write(".map(")
        writeParameterList(s.from)
        write(" => new { ")
        var first = true
        s.properties.foreach { p =>
          if (!first)
            write("; ")
          first = false
          write("val ")
          write(p.name)
          write(" = ")
          visit(p.e)
        }
        write(" })")
    }
    s
  }

  protected override def visitIn(i: In): Expression = {
    i.values match {
      case Right(seq) =>
        write("Seq(")
        val size = seq.size
        if (size > math.max(2, math.min(MaxThreshold, listAbbreviationThreshold))) {
          visit(seq.head)
          write(s"... [${size - 2} elements]... ")
          visit(seq(size - 1))
        } else {
          var first = true
          seq.foreach { ex =>
            if (!first) write(", ")
            first = false
            visit(ex)
          }
        }
        write(")")
      case Left(s) => visit(s)
    }
    write(".contains(")
    visit(i.e)
    write(")")
    i
  }

  protected def writeBinary(operator: String, left: Expression, right: Expression) = {
    visit(left)
    write(' ')
    write(operator)
    write(' ')
    visit(right)
  }

  protected override def visitBinary(b: Binary): Expression = {
    import BinaryOperator._
    b.op match {
      case Equal              => writeBinary("==", b.left, b.right)
      case LessThan           => writeBinary("<", b.left, b.right)
      case LessThanOrEqual    => writeBinary("<=", b.left, b.right)
      case GreaterThan        => writeBinary(">", b.left, b.right)
      case GreaterThanOrEqual => writeBinary(">=", b.left, b.right)
      case AndAlso            => writeBinary("&&", b.left, b.right)
      case OrElse             => writeBinary("||", b.left, b.right)
      case _ => throw new UnsupportedOperationException(s"Unsupported server side binary expression type: ${b.op}")
    }
    b
  }

  protected override def visitConstant(c: Constant): Expression = {
    writeConstant(c.value)
    c
  }

  protected override def visitFunction(f: Function): Expression = {
    f match {
      case Function(CollectionOps.Contains.Name, List(p: Property, c: Constant)) =>
        visitProperty(p)
        write(".contains(")
        visitConstant(c)
        write(")")
        f
      case _ =>
        super.visitFunction(f)
    }
  }

  protected override def visitProperty(p: Property): Expression = {
    if ((p.owner ne null)) {
      write(getParameterName(p.owner))
      write(".")
    }
    if (p.names.size == 1)
      write(p.names.head)
    else
      write(p.names.mkString("[", ",", "]"))
    p
  }

  protected def visitSource(source: QuerySource): Expression = {
    source match {
      case e: Entity                => writeEntitySource(e)
      case c: EntityBitemporalSpace => writeEntityBitemporalSpaceSource(c)
      case _                        => throw new UnsupportedOperationException(s"Unsupported query source: $source")
    }
    source
  }

  protected def writeEntityBitemporalSpaceSource(e: EntityBitemporalSpace): Unit = {
    write("fromBitemporalSpace(")
    write(e.name)
    if (e.when ne null) {
      write(", ")
      write(e.when)
    }
    write(", ")
    write(e.kind)
    write(")")
  }

  protected def writeEntitySource(e: Entity): Unit = {
    write("from(")
    write(e.name)
    if (e.when ne null) {
      write(", ")
      write(e.when)
    }
    write(")")
  }

  protected def addIds(e: QuerySource): Unit = {
    e match {
      case e: Entity                => addId(e.id)
      case e: Event                 => addId(e.id)
      case e: Select                => addId(e.id)
      case e: EntityBitemporalSpace => addId(e.id)
      case e: Join =>
        addIds(e.left)
        addIds(e.right)
    }
  }

  protected def addId(id: Id): Unit = {
    if (ids.get(id) eq null) {
      val name = "t" + ids.size
      ids.put(id, name)
    }
  }

  protected def writeConstant(value: Any): Unit = {
    value match {
      case s: String =>
        write('"')
        write(s)
        write('"')
      case sk: SerializedKey =>
        if (sk.properties.size == 1)
          writeConstant(sk.properties.head._2)
        else {
          write("SK(")
          write(sk.properties.mkString(","))
          write(")")
        }
      case s: Seq[_] =>
        write("Seq(")
        write(s.mkString(","))
        write(")")
      case k: Key[_] =>
        writeConstant(k.toSerializedKey)
      case _ =>
        write(value)
    }
  }

  protected def write(value: Any): Unit = {
    sb.append(value)
  }

  protected def writeParameterList(src: QuerySource): Unit = {
    val ids = new mutable.ArrayBuffer[Id](2)
    def collectIds(s: QuerySource): Unit = {
      s match {
        case e: Entity                => ids.append(e.id)
        case e: Event                 => ids.append(e.id)
        case e: Select                => ids.append(e.id)
        case e: EntityBitemporalSpace => ids.append(e.id)
        case e: Join =>
          collectIds(e.left)
          collectIds(e.right)
      }
    }
    collectIds(src)
    if (ids.size == 1) write(getParameterName(ids(0)))
    else write(ids.map(getParameterName).mkString("(", ",", ")"))
  }

  protected def getParameterName(id: Id): String = {
    var name = ids.get(id)
    if (name == null) {
      name = "p" + id.hashCode + "?"
      ids.put(id, name)
    }
    name
  }
}

object ExpressionPriQLFormatter {
  val MaxThreshold: Int = Int.MaxValue - 1
  lazy val DefaultThreshold: Int =
    DiagnosticSettings.getIntProperty("optimus.priql.print.expressionListAbbreviationThreshold", 10)
  def format(e: Expression, threshold: Int = DefaultThreshold): String = {
    val formatter = new ExpressionPriQLFormatter(threshold)
    formatter.visit(e)
    formatter.toString
  }

  def formatSampling(e: Expression, threshold: Int = DefaultThreshold): String = {
    val formatter = new SamplingFormatter(threshold)
    formatter.visit(e)
    formatter.toString
  }

  private class SamplingFormatter(t: Int) extends ExpressionPriQLFormatter(t) {
    override protected def formatTake(take: Option[Expression]): Unit = {
      take.foreach(_ match {
        case Constant(num: Int, _) =>
          write(s".sample($num)")
        case x =>
          throw new UnsupportedOperationException(s"Unsupported server side 'sample': $x")
      })
    }
  }
}
