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
package optimus.platform

import msjava.slf4jutils.scalalog.getLogger
import optimus.core.CoreAPI._
import optimus.graph._
import optimus.platform.annotations.pluggable
import optimus.platform.relational._
import optimus.platform.relational.aggregation._
import optimus.platform.relational.inmemory.QueryExplainGenerator
import optimus.platform.relational.internal.ExcelHeader.ExcelHeader
import optimus.platform.relational.internal._
import optimus.platform.relational.pivot.PivotTable
import optimus.platform.relational.pivot.PivotTableMacros
import optimus.platform.relational.serialization.QueryBuilder
import optimus.platform.relational.tree._
import optimus.platform.storable.Entity
import optimus.utils.datetime.ZoneIds
import org.apache.poi.ss.usermodel.Sheet

import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.io.PrintWriter
import java.time.ZoneId
import scala.collection.mutable
import scala.math.Ordering
import scala.util.control.NonFatal

// NOTE ABOUT TYPESAFETY:
//
// Priql is not always as typesafe as it seems. Many of the methods here create their result types at runtime. This can
// end up creating illegal types which only fail at runtime. For example,
//
// query[T].replace[A](...)
//
// creates a new type T' defined as (T with A). If T is a final class, then T' will produce an error *at runtime*. I've
// added a check for this (in QueryMacros, see OPTIMUS-50319) but the check won't work if .replace[A] is called with a
// type parameter (e.g. in a generic function).
//
// Please keep in mind this issue when extending Query. If you add new type of queries and they require type extensions
// make sure to do so through macros and add the appropriate checks.

/**
 * This trait defines the set of standard query operators that follow the PRIQL pattern and enable user to express
 * traversal, filter, and projection operations over relational data.
 *
 * The majority of the methods in this traits are defined as Scala macro to lift the lambda into an expression tree and
 * delegate to its 'provider' which has the real implementation.
 */
trait Query[+T] extends Serializable {
  val element: MultiRelationElement
  val provider: QueryProvider

  /**
   * The element type of this relation. Since 'Query' is covariant, the elementType always satisfies the rule:
   * elementType <:< typeInfo[T]
   */
  def elementType: TypeInfo[_] = element.rowTypeInfo

  final def permitTableScan(implicit pos: MethodPosition): Query[T] = provider.permitTableScan(this)(pos)

  /**
   * It marks the current DAL query as Silent Failure, i.e. return entitled-only records. This method is allow only
   * immediately after from(...) for DAL PriQL except count() method. Any query caused by Lambda in filter/map etc. is
   * not impacted by this flag.
   * @param pos
   * @return
   */
  final def entitledOnly(implicit pos: MethodPosition): Query[T] = provider.entitledOnly(this)(pos)

  /**
   * Projects each item of a relation into a new form using the selector function 'f'
   */
  final def map[U](f: T => U)(implicit resultType: TypeInfo[U], pos: MethodPosition): Query[U] =
    macro QueryMacros.map[T, U]

  /**
   * Projects each item of a relation to an 'F[U]' which could be further converted to relation using 'conv' and
   * combines the resulting relations into one relation
   */
  final def flatMap[U, F[_]](
      f: T => F[U])(implicit conv: QueryConverter[U, F], resultType: TypeInfo[U], pos: MethodPosition): Query[U] =
    macro QueryMacros.flatMap[T, U, F]

  /**
   * Filters a relation of items based on predicator 'p'
   */
  final def filter(p: T => Boolean)(implicit pos: MethodPosition): Query[T] = macro QueryMacros.filter[T]

  /**
   * Same as filter method in this implementation, invoked by 'for comprehension'
   */
  final def withFilter(p: T => Boolean)(implicit pos: MethodPosition): Query[T] = macro QueryMacros.filter[T]

  /**
   * Groups the items of a relation according to a specified key selector function 'f'.
   */
  final def groupBy[U](f: T => U)(implicit keyType: TypeInfo[U], pos: MethodPosition): GroupQuery[U, Query[T]] =
    macro QueryMacros.groupBy[T, U]

  /**
   * Groups the items of a relation according to a specified key type GroupKeyType and generate the corresponding sub
   * group of type GroupValueType which belongs to a key
   */
  final def groupBy[GroupKeyType >: T, GroupValueType >: T](implicit
      groupKeyType: TypeInfo[GroupKeyType],
      groupValueType: TypeInfo[GroupValueType],
      pos: MethodPosition): GroupQuery[GroupKeyType, Query[GroupValueType]] =
    provider.groupBy[T, GroupKeyType, GroupValueType](this) // (groupKeyType, groupValueType, pos)

  /**
   * Aggregate the items: first group them according to GroupKeyType then applying f to sub group of GroupValueType to
   * generate a value of AggregateType
   */
  final def aggregateBy[GroupKeyType >: T, GroupValueType >: T, AggregateType](
      f: Query[GroupValueType] => AggregateType)(implicit
      groupKeyType: TypeInfo[GroupKeyType],
      groupValueType: TypeInfo[GroupValueType],
      aggregateType: TypeInfo[AggregateType],
      pos: MethodPosition): Query[GroupKeyType with AggregateType] =
    macro QueryMacros.aggregateByExplicit[T, GroupKeyType, GroupValueType, AggregateType]

  /**
   * Aggregate the items: first group them according to GroupKeyType then applying implicit aggregators provided by user
   * to sub group of GroupValueType to generate a value of AggregateType
   */
  final def aggregateByImplicit[GroupKeyType >: T, GroupValueType >: T, AggregateType](implicit
      groupKeyType: TypeInfo[GroupKeyType],
      groupValueType: TypeInfo[GroupValueType],
      aggregateType: TypeInfo[AggregateType],
      pos: MethodPosition): Query[GroupKeyType with AggregateType] =
    macro QueryMacros.aggregateByImplicit[T, GroupKeyType, GroupValueType, AggregateType]

  /**
   * Aggregate the untyped items: first group them according to groupByParameters then using provided Aggregator to
   * generate a Dynamic object which contains both group value and aggregate value
   */
  final def aggregateByUntyped(groupByProperties: Set[String], untypedAggregations: UntypedAggregation*)(implicit
      pos: MethodPosition): Query[DynamicObject] =
    provider.aggregateByUntyped(this.untype, groupByProperties, untypedAggregations: _*)(pos)

  /**
   * Inner joins the elements of two relations based on the following 'on' lambda (or using the default keys in both
   * relations)
   */
  final def innerJoin[U](right: Query[U])(implicit pos: MethodPosition): JoinQuery[T, U] =
    provider.innerJoin(this, right)(pos)

  /**
   * Left outer joins the elements of two relations based on the following 'on' lambda (or using the default keys in
   * both relations). The right default value is either 'null' or provided by following 'withRightDefault' method
   */
  final def leftOuterJoin[U](right: Query[U])(implicit pos: MethodPosition): JoinQuery[T, U] =
    provider.leftOuterJoin(this, right)(pos)

  /**
   * Right outer joins the elements of two relations based on the following 'on' lambda (or using the default keys in
   * both relations). The left default value is either 'null' or provided by following 'withLeftDefault' method
   */
  final def rightOuterJoin[U](right: Query[U])(implicit pos: MethodPosition): JoinQuery[T, U] =
    provider.rightOuterJoin(this, right)(pos)

  /**
   * Full outer joins the elements of two relations based on the following 'on' lambda (or using the default keys in
   * both relations). The right default value is either 'null' or provided by following 'withRightDefault' method. The
   * left default value is either 'null' or provided by following 'withLeftDefault' method
   */
  final def fullOuterJoin[U](right: Query[U])(implicit pos: MethodPosition): JoinQuery[T, U] =
    provider.fullOuterJoin(this, right)(pos)

  /**
   * Produces the set union of multiple queries by using the default equality-comparer/relation-key
   */
  final def union[U >: T](others: Query[U]*)(implicit pos: MethodPosition): Query[U] =
    provider.union[U](this, others: _*)(pos)

  /**
   * Similar to union, but performs aggregations on items with a same key (ModuloKey[U, V]). Both U and V should be
   * traits that:
   *   1. there are no 'val's in the trait
   *
   * 2. "@ProxyFinalModifier final def" is allowed
   *
   * 3. other 'def's in the trait should have NO implementation
   *
   * 4. 'def' with non-zero arguments will be delegated to the 1st item of the items with a same key
   */
  final def merge[U >: T, V >: U](others: Query[U]*)(ag: Aggregator[V] { type Result = V })(implicit
      pos: MethodPosition): Query[U] = provider.merge[U, V](this, others: _*)(ag)(pos)

  /**
   * The difference operator will pick up all elements in current relation but not in the 'other' relation
   */
  final def difference[U >: T](other: Query[U])(implicit pos: MethodPosition): Query[U] =
    provider.difference[U](this, other)(pos)

  /**
   * Turns a strong typed relation into dynamic typed relation
   */
  final def untype(implicit pos: MethodPosition): Query[DynamicObject] =
    if (elementType <:< classOf[DynamicObject]) this.asInstanceOf[Query[DynamicObject]]
    else provider.untype[T](this)(pos)

  /**
   * The extend operator will convert the relation to a dynamic type and extend it.
   */
  final def extend[U](field: String, f: DynamicObject => U)(implicit
      fieldType: TypeInfo[U],
      pos: MethodPosition): Query[DynamicObject] = macro QueryMacros.extend[T, U]

  /**
   * The shapeTo operator will shape one relation to another type and at the same time retain the columns of the
   * original type.
   */
  final def shapeTo[U](f: DynamicObject => U)(implicit shapeToType: TypeInfo[U], pos: MethodPosition): Query[U] =
    macro QueryMacros.shapeTo[T, U]

  /**
   * We have some limitations: all types have to be trait, otherwise it will throw RuntimeException.
   *
   * T: could be: A with B with C with ..., all type should be interface or trait.
   *
   * U : could be: A with B with ..., all types should be interface or trait.
   *
   * E.g.
   *
   * {{{
   * trait ExtendTrait
   * trait MyResult
   * class MyResultImpl
   * }}}
   *
   * Note: we have to explicitly convert type from MyResultImpl to MyResult
   *
   * {{{
   * val data = Set(MyResultImpl(1),MyResultImpl(2))
   * val relation : Relation[MyResult] = from(data)
   * val q = relation.extendTyped(r => new ExtendTrait{def value=r.v})
   * }}}
   */
  final def extendTyped[U](f: T => U)(implicit extensionType: TypeInfo[U], pos: MethodPosition): Query[T with U] =
    macro QueryMacros.extendTyped[T, U]

  /**
   * Same as extendTyped
   */
  final def extendTypedValue[U](extVal: U)(implicit extensionType: TypeInfo[U], pos: MethodPosition): Query[T with U] =
    macro QueryMacros.extendTypedValue[T, U]

  /**
   * The replace operator will replace the value of existing coordinates in the relation.
   */
  final def replace[U >: T](f: T => U)(implicit replaceType: TypeInfo[U], pos: MethodPosition): Query[T] =
    macro QueryMacros.replace[T, U]

  /**
   * Same as replace operator
   */
  final def replaceValue[U >: T](repVal: U)(implicit replaceType: TypeInfo[U], pos: MethodPosition): Query[T] =
    macro QueryMacros.replaceValue[T, U]

  /**
   * Same as TabularBase sortBy
   */
  final def sortBy[U](f: T => U)(implicit ordering: Ordering[U], sortType: TypeInfo[U], pos: MethodPosition): Query[T] =
    macro QueryMacros.sortBy[T, U]

  /**
   * A newly added operator, it could guarantee the absolute order among rows.
   *
   * So if two rows have same sorted key values, their relative order could be decided by this operator. If we call take
   * operator later, RT could be guaranteed too.
   *
   * There are some rules to guarantee absolute order:
   *
   * 1 a source itself could be guaranteed by nature e.g Seq in memory otherwise
   *
   * 2 sort on priql key
   */
  final def arrange(implicit pos: MethodPosition): Query[T] = provider.arrange[T](this)(pos)

  /**
   * Returns distinct elements from a sequence by using the default equality comparer
   */
  final def distinct(implicit pos: MethodPosition): Query[T] = provider.distinct[T](this)(pos)

  /**
   * The take operator will take num rows starting from offset
   */
  final def take(offset: Int, num: Int)(implicit pos: MethodPosition): Query[T] = provider.take(this, offset, num)(pos)
  final def takeFirst(num: Int)(implicit pos: MethodPosition): Query[T] = provider.take(this, 0, num)(pos)
  final def takeRight(num: Int)(implicit pos: MethodPosition): Query[T] = provider.take(this, -1, num)(pos)

  /**
   * return PivotQuery and do pivot calculation later on this pivot query
   */
  final def pivotOn[U](pivotOnLambda: T => U): Any = macro PivotTableMacros.pivotOn[T, U, T]

  final def pivotOnTyped[PivotOnType >: T]: Any = macro PivotTableMacros.pivotOnTyped[T, PivotOnType, T]

  /**
   * Returns the aggregate value of a sequence of T according to given aggregator
   */
  final def aggregate[U >: T](aggregator: Aggregator[U]): aggregator.Result = macro QueryMacros.aggregate1[T, U]

  /**
   * Invokes a projection function on each element and returns the aggregate resulting value according to given
   * aggregator
   */
  final def aggregate[U](f: T => U)(aggregator: Aggregator[U])(implicit aggregateType: TypeInfo[U]): aggregator.Result =
    macro QueryMacros.aggregate2[T, U]

  final def reduce[U >: T](op: (U, U) => U): U = macro QueryMacros.reduce[T, U]

  final def foldLeft[U](z: U)(op: (U, T) => U): U = macro QueryMacros.foldLeft[T, U]

  @async final def sum[U >: T](implicit sum: Sum[U]): U = provider.sum[U](this)(sum)

  final def sum[U](f: T => U)(implicit sum: Sum[U]): U = macro QueryMacros.sum[T, U]

  /**
   * Computes the average of a sequence of T values
   */
  final def average[U >: T](implicit avg: Avg[U]): avg.Result = macro QueryMacros.average1[T, U]

  /**
   * Computes the average of a sequence of T values that is obtained by invoking a projection function on each element
   * of the input sequence
   */
  final def average[U](f: T => U)(implicit avg: Avg[U]): avg.Result = macro QueryMacros.average2[T, U]

  /**
   * Compute the variance of a sequence of T values (implicit value defaults to sample variance)
   */
  final def variance[U >: T](implicit v: Variance[U]): v.Result = macro QueryMacros.variance1[T, U]

  final def variance[U](f: T => U)(implicit v: Variance[U]): v.Result = macro QueryMacros.variance2[T, U]

  /**
   * Compute the standard deviation of a sequence of T values (implicit value defaults to sample stddev)
   */
  final def stddev[U >: T](implicit stdDev: StandardDeviation[U]): stdDev.Result = macro QueryMacros.stddev1[T, U]

  final def stddev[U](f: T => U)(implicit stdDev: StandardDeviation[U]): stdDev.Result = macro QueryMacros.stddev2[T, U]

  /**
   * Correlation coefficient (default to use PearsonsCorrelation)
   */
  final def corr(x: T => Double, y: T => Double)(implicit cor: Correlation[Double]): Double = macro QueryMacros.corr[T]

  /**
   * Returns the minimum value of a sequence of T
   */
  @async final def min[U >: T](implicit min: Min[U]): U = provider.min[U](this)(min)

  /**
   * Invokes a projection function on each element and returns the minimum resulting value
   */
  final def min[U](f: T => U)(implicit min: Min[U]): U = macro QueryMacros.min[T, U]

  /**
   * Returns the maximum value of a sequence of T
   */
  @async final def max[U >: T](implicit max: Max[U]): U = provider.max[U](this)(max)

  /**
   * Invokes a projection function on each element and returns the maximum resulting value
   */
  final def max[U](f: T => U)(implicit max: Max[U]): U = macro QueryMacros.max[T, U]

  /**
   * Returns a Long that represents the total number of elements in a sequence
   */
  @async final def count: Long = provider.count(this)

  /**
   * Returns a Long that represents the number of elements in a sequence that satisfy a condition
   */
  final def count(f: T => Boolean): Long = macro QueryMacros.count[T]

  /**
   * Returns the first element in a sequence
   */
  @async final def head: T = provider.head(this)

  /**
   * Optionally returns the first element in a sequence
   */
  @async final def headOption: Option[T] = provider.headOption(this)

  /**
   * Returns the last element in a sequence
   */
  @async final def last: T = provider.last(this)

  /**
   * Optionally returns the last element in a sequence
   */
  @async final def lastOption: Option[T] = provider.lastOption(this)

  @async def execute: Iterable[T] = Query.execute(this)

  protected def writeReplace: Object = {
    import optimus.platform.Query.QueryMemento
    new QueryMemento(this)
  }
}

/**
 * Defines extra methods for join operator
 */
trait JoinQuery[+L, +R] extends Query[(L, R)] {
  def on(f: (L, R) => Boolean): JoinQuery[L, R] = macro QueryMacros.on[L, R]
  def on(fields: Seq[String]): JoinQuery[L, R] = provider.on(this, fields)
  def onNatural: NaturalJoinQuery[L, R] = macro QueryMacros.onNatural[L, R]

  /**
   * since L is covariant, we cannot put R => L in withLeftDefault's input parameter. It makes sense to outer join, if
   * it is right outer join [A1, B1] which A1 derives from A, B1 derives from B, while left default lambda is A1 => B2
   * (B2 derives B), some output will be [A1, B2] which is incorrect. But it could be [A1, B].
   */
  def withLeftDefault[B >: L](f: R => B)(implicit b: TypeInfo[B], pos: MethodPosition): JoinQuery[B, R] =
    macro QueryMacros.withLeftDefault[B, R]
  def withRightDefault[B >: R](f: L => B)(implicit b: TypeInfo[B], pos: MethodPosition): JoinQuery[L, B] =
    macro QueryMacros.withRightDefault[L, B]
}

trait NaturalJoinQuery[+L, +R] extends Query[(L with R)] {
  def withLeftDefault[B >: L](f: R => B)(implicit b: TypeInfo[B], pos: MethodPosition): NaturalJoinQuery[B, R] =
    macro QueryMacros.withLeftDefaultNatural[B, R]
  def withRightDefault[B >: R](f: L => B)(implicit b: TypeInfo[B], pos: MethodPosition): NaturalJoinQuery[L, B] =
    macro QueryMacros.withRightDefaultNatural[B, R]
}

/**
 * Defines extra methods for groupBy operator
 */
trait GroupQuery[+T, +U] extends Query[(T, U)] {

  /**
   * Transforms this group relation by applying 'f' to every value.
   */
  final def mapValues[S](f: U => S)(implicit resultType: TypeInfo[S], pos: MethodPosition): GroupQuery[T, S] =
    macro QueryMacros.mapValues[T, U, S]
}

/**
 * The Query object is a key part of the priql system.
 *
 * Query provides methods that can be used to perform actions on the unmaterialised Relation or Table object (see their
 * documentation for more detail).
 *
 * Query represents the starting point for any kind of translation from the abstract unmaterialised to the concrete
 * form.
 *
 * Query also provides methods to do general interrogation of the unmaterialised form (examining the query plan for
 * example), persisting/fetching a query.
 */
object Query {
  import optimus.platform.relational.formatter._

  private[optimus] def ReifiedTarget = AbstractLambdaReifier.ReifiedTarget

  /** Serializable representation of a Query */
  private final case class QueryMemento(src: QueryBuilder[_]) {
    def this(query: Query[_]) = this(QueryBuilder.of(query.element))
    private def readResolve: Object = src.build
  }

  implicit class LegacyGroupQueryOps[T, U](val origin: GroupQuery[T, Query[U]]) extends AnyVal {
    final def expandGroup: GroupQuery[T, Iterable[U]] = {
      import optimus.platform.relational.internal.OptimusCoreAPI._
      implicit val uType = origin.elementType.typeParams(1).typeParams(0).cast[U]
      val f = liftNode { (q: Query[Any]) =>
        q.provider.execute[Iterable[Any]](q.element).apply()
      }
      origin.provider.mapValues(origin, Lambda1(None, Some(f.asInstanceOf[Query[U] => Node[Iterable[U]]])))
    }
  }

  def findShapeType(e: RelationElement): TypeInfo[_] = {
    e match {
      case m: MultiRelationElement => m.rowTypeInfo
      case _                       => findShapeType(e.rowTypeInfo)
    }
  }

  def findShapeType(t: TypeInfo[_]): TypeInfo[_] = {
    if (t <:< classOf[Query[_]] || ((t.clazz ne null) && t.clazz.isArray)) {
      t.typeParams.headOption.getOrElse(TypeInfo.ANY)
    } else if (t <:< IterableClass) {
      if (t.typeParams.isEmpty) TypeInfo.ANY
      if (t.typeParams.size == 1) t.typeParams.head
      else {
        throw new RelationalUnsupportedException(s"Unsupported collection type $t")
      }
    } else t
  }

  def flattenBOOLANDConditions(element: RelationElement): List[RelationElement] = {
    if (element eq null) Nil
    else {
      var s: List[RelationElement] = element :: Nil
      val b = new mutable.ListBuffer[RelationElement]
      while (s.nonEmpty) {
        val pop = s.head
        s = s.tail
        pop match {
          case BinaryExpressionElement(BinaryExpressionType.BOOLAND, left, right, _) =>
            s = left :: right :: s
          case e => b += e
        }
      }
      b.result()
    }
  }

  @async def execute[T, U](query: GroupQuery[T, Query[U]]): Map[T, Iterable[U]] = {
    val q = query.expandGroup
    q.provider.execute[Iterable[(T, Iterable[U])]](q.element).apply().toMap
  }
  @async def execute[T](query: Query[T]): Iterable[T] = {
    query.provider.execute[Iterable[T]](query.element).apply()
  }

  @async def executeColumn[T <: Entity, U](query: Query[T])(implicit conversion: ColumnViewToColumnResult[T, U]): U = {
    val rowResults = query.execute
    val columnView = RelationalUtils.convertEntityRowDataToColumnData(rowResults)(query.elementType.cast[T])
    conversion.convert(columnView)
  }

  // used in ExecutionProvider
  @entity object Tracer {
    @pluggable @node def trace[R](pos: MethodPosition, nf: NodeFunction0[R]): R = nf()

    if (Settings.showPriqlMethodInfo)
      trace_info.setPlugin(priqlTracePlugin)
    trace_info.setCacheable(false)
  }

  // ----------------------------------------------------------------------------------------------------------------------
  // ---------------------------------------- Relation Output API              --------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------------

  @async def display[RowSeq: TableDisplay.Tabulator](rowSeq: RowSeq)(implicit settings: OutputSettings): Unit =
    TableDisplay.format(rowSeq)
  @async def displayTo[RowSeq: TableDisplay.Tabulator](
      rowSeq: RowSeq,
      out: OutputStream,
      footer: Option[String] = None)(implicit settings: OutputSettings): Unit = {
    TableDisplay.format(rowSeq, out)
    footer.foreach { f =>
      val pw = new PrintWriter(out)
      pw.print(f) // not println as we assume the footer has what it intends
      pw.close()
    }
  }
  @async def displayAsString[RowSeq <: Query[_]: TableDisplay.Tabulator](rowSeq: RowSeq)(implicit
      settings: OutputSettings): String = rowSeq.untype.execute
    // for now we do not include which coordinate traits this is implementing, only the actual field names and values
    .map {
      _.getAll
        .map { case (k, v) =>
          s"$k=$v"
        }
        .mkString("[", ",", "]")
    }
    .mkString("\n")

  /**
   * Displays 2 relationals of different shapes in a single shot guaranteeing they will appear together in the output
   * stream.
   */
  @async def display2[RowSeq1: TableDisplay.Tabulator, RowSeq2: TableDisplay.Tabulator](
      header: String,
      footer: Option[String],
      rowSeq1: RowSeq1,
      rowSeq2: RowSeq2,
      out: OutputStream = TableDisplay.defaultOutput)(implicit settings: OutputSettings): Unit = {
    displayToImpl(
      out,
      header,
      asNode { out =>
        Query.displayTo[RowSeq1](rowSeq1, out, footer)
        Query.displayTo[RowSeq2](rowSeq2, out, footer)
      })
  }

  /**
   * Displays 3 relationals of different shapes in a single shot, guaranteeing they will appear together in the output
   * stream.
   */
  @async def display3[
      RowSeq1: TableDisplay.Tabulator,
      RowSeq2: TableDisplay.Tabulator,
      RowSeq3: TableDisplay.Tabulator](
      header: String,
      footer: Option[String],
      rowSeq1: RowSeq1,
      rowSeq2: RowSeq2,
      rowSeq3: RowSeq3,
      out: OutputStream = TableDisplay.defaultOutput)(implicit settings: OutputSettings): Unit = {
    displayToImpl(
      out,
      header,
      asNode { out =>
        Query.displayTo[RowSeq1](rowSeq1, out, footer)
        Query.displayTo[RowSeq2](rowSeq2, out, footer)
        Query.displayTo[RowSeq3](rowSeq3, out, footer)
      }
    )
  }

  /**
   * Displays 4 relationals of different shapes in a single shot, guaranteeing they will appear together in the output
   * stream.
   */
  @async def display4[
      RowSeq1: TableDisplay.Tabulator,
      RowSeq2: TableDisplay.Tabulator,
      RowSeq3: TableDisplay.Tabulator,
      RowSeq4: TableDisplay.Tabulator](
      header: String,
      footer: Option[String],
      rowSeq1: RowSeq1,
      rowSeq2: RowSeq2,
      rowSeq3: RowSeq3,
      rowSeq4: RowSeq4,
      out: OutputStream = TableDisplay.defaultOutput)(implicit settings: OutputSettings): Unit = {
    displayToImpl(
      out,
      header,
      asNode { out =>
        Query.displayTo[RowSeq1](rowSeq1, out, footer)
        Query.displayTo[RowSeq2](rowSeq2, out, footer)
        Query.displayTo[RowSeq3](rowSeq3, out, footer)
        Query.displayTo[RowSeq4](rowSeq4, out, footer)
      }
    )
  }

  /**
   * Displays 5 relationals of different shapes in a single shot, guaranteeing they will appear together in the output
   * stream.
   */
  @async def display5[
      RowSeq1: TableDisplay.Tabulator,
      RowSeq2: TableDisplay.Tabulator,
      RowSeq3: TableDisplay.Tabulator,
      RowSeq4: TableDisplay.Tabulator,
      RowSeq5: TableDisplay.Tabulator](
      header: String,
      footer: Option[String],
      rowSeq1: RowSeq1,
      rowSeq2: RowSeq2,
      rowSeq3: RowSeq3,
      rowSeq4: RowSeq4,
      rowSeq5: RowSeq5,
      out: OutputStream = TableDisplay.defaultOutput)(implicit settings: OutputSettings): Unit = {
    displayToImpl(
      out,
      header,
      asNode { out =>
        Query.displayTo[RowSeq1](rowSeq1, out, footer)
        Query.displayTo[RowSeq2](rowSeq2, out, footer)
        Query.displayTo[RowSeq3](rowSeq3, out, footer)
        Query.displayTo[RowSeq4](rowSeq4, out, footer)
        Query.displayTo[RowSeq5](rowSeq5, out, footer)
      }
    )
  }

  /**
   * Displays multiple relationals of the same shape in a single shot, guaranteeing they will appear together in the
   * output stream.
   */
  @async def displaySameShape[RowSeq: TableDisplay.Tabulator](
      header: String,
      footer: Option[String],
      rowSeqs: Seq[RowSeq],
      out: OutputStream = TableDisplay.defaultOutput)(implicit settings: OutputSettings): Unit = {
    displayToImpl(
      out,
      header,
      asNode { out =>
        rowSeqs.foreach(Query.displayTo[RowSeq](_, out, footer))
      })
  }

  /** The following variants of display don't throw any exceptions and log the stacktrace instead */
  @async def displaySafe[RowSeq: TableDisplay.Tabulator](rowSeq: RowSeq)(implicit settings: OutputSettings): Unit =
    evaluateSafe(asNode { () =>
      display(rowSeq)
    })
  @async def displaySafe[RowSeq: TableDisplay.Tabulator](
      rowSeq: RowSeq,
      header: String,
      footer: Option[String],
      out: OutputStream = TableDisplay.defaultOutput)(implicit settings: OutputSettings): Unit =
    evaluateSafe(asNode { () =>
      displayToImpl(
        out,
        header,
        asNode { out =>
          Query.displayTo[RowSeq](rowSeq, out, footer)
        })
    })
  @async def displayToSafe[RowSeq: TableDisplay.Tabulator](rowSeq: RowSeq, out: OutputStream)(implicit
      settings: OutputSettings): Unit =
    evaluateSafe(asNode { () =>
      displayTo(rowSeq, out)
    })
  @async def display2Safe[RowSeq1: TableDisplay.Tabulator, RowSeq2: TableDisplay.Tabulator](
      header: String,
      footer: Option[String],
      rowSeq1: RowSeq1,
      rowSeq2: RowSeq2,
      out: OutputStream = TableDisplay.defaultOutput)(implicit settings: OutputSettings): Unit =
    evaluateSafe(asNode { () =>
      display2(header, footer, rowSeq1, rowSeq2, out)
    })
  @async def display3Safe[
      RowSeq1: TableDisplay.Tabulator,
      RowSeq2: TableDisplay.Tabulator,
      RowSeq3: TableDisplay.Tabulator](
      header: String,
      footer: Option[String],
      rowSeq1: RowSeq1,
      rowSeq2: RowSeq2,
      rowSeq3: RowSeq3,
      out: OutputStream = TableDisplay.defaultOutput)(implicit settings: OutputSettings): Unit =
    evaluateSafe(asNode { () =>
      display3(header, footer, rowSeq1, rowSeq2, rowSeq3, out)
    })
  @async def display4Safe[
      RowSeq1: TableDisplay.Tabulator,
      RowSeq2: TableDisplay.Tabulator,
      RowSeq3: TableDisplay.Tabulator,
      RowSeq4: TableDisplay.Tabulator](
      header: String,
      footer: Option[String],
      rowSeq1: RowSeq1,
      rowSeq2: RowSeq2,
      rowSeq3: RowSeq3,
      rowSeq4: RowSeq4,
      out: OutputStream = TableDisplay.defaultOutput)(implicit settings: OutputSettings): Unit =
    evaluateSafe(asNode { () =>
      display4(header, footer, rowSeq1, rowSeq2, rowSeq3, rowSeq4, out)
    })
  @async def display5Safe[
      RowSeq1: TableDisplay.Tabulator,
      RowSeq2: TableDisplay.Tabulator,
      RowSeq3: TableDisplay.Tabulator,
      RowSeq4: TableDisplay.Tabulator,
      RowSeq5: TableDisplay.Tabulator](
      header: String,
      footer: Option[String],
      rowSeq1: RowSeq1,
      rowSeq2: RowSeq2,
      rowSeq3: RowSeq3,
      rowSeq4: RowSeq4,
      rowSeq5: RowSeq5,
      out: OutputStream = TableDisplay.defaultOutput)(implicit settings: OutputSettings): Unit =
    evaluateSafe(asNode { () =>
      display5(header, footer, rowSeq1, rowSeq2, rowSeq3, rowSeq4, rowSeq5, out)
    })
  @async def displaySameShapeSafe[RowSeq: TableDisplay.Tabulator](
      header: String,
      footer: Option[String],
      rowSeqs: Seq[RowSeq],
      out: OutputStream = TableDisplay.defaultOutput)(implicit settings: OutputSettings): Unit =
    evaluateSafe(asNode { () =>
      displaySameShape(header, footer, rowSeqs, out)
    })

  @async private def evaluateSafe(f: NodeFunction0[Unit]): Unit =
    asyncResult { f() } valueOrElse {
      case NonFatal(e) => log.error("query display function threw in safe mode, suppressing exception", e)
      case fatal       => throw fatal
    }

  @async private def displayToImpl(out: OutputStream, header: String, dump: NodeFunction1[OutputStream, Unit]): Unit = {
    val data = {
      val temp = new ByteArrayOutputStream
      dump(temp)
      temp.close
      s"$header\n${temp.toString}" // not worrying about charset for now, add logic if needed
    }
    val writer = new PrintWriter(out)
    writer.print(data) // not printLN as we assume that was handled by delegation to #displayTo
    writer.flush // do not close as we do not have ownership of the passed-in stream and that might close it, flush to be sure all is visible post call
  }

  @async def csvWrite[RowSeq: Csv.Tabulator](rowSeq: RowSeq)(implicit settings: OutputSettings) = Csv.format(rowSeq)
  @async def csvWriteTo[RowSeq: Csv.Tabulator](rowSeq: RowSeq, out: OutputStream)(implicit settings: OutputSettings) =
    Csv.format(rowSeq, out)

  val log = getLogger("optimus.platform.Query")

  def printQueryExplain[T](query: Query[T]): String = {
    val e: RelationElement = query.element
    val elem = DefaultQueryProvider.optimizeTree(e)
    QueryExplainGenerator.generate(elem)
  }

  // ----------------------------------------------------------------------------------------------------------------------
  // ---------------------------------------- Excel Output API                 --------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------------
  @async def fillExcelSheet[T: TypeInfo](
      query: Query[T],
      sheet: Sheet,
      hasHead: ExcelHeader,
      targetTimeZone: ZoneId = ZoneIds.UTC): Unit = {
    val q = query match {
      case gq: GroupQuery[_, _] if gq.elementType.typeParams(1) <:< classOf[Query[_]] =>
        LegacyGroupQueryOps(gq.asInstanceOf[GroupQuery[Any, Query[Any]]]).expandGroup
      case _ =>
        query
    }
    q.elementType match {
      case TupleTypeInfo(lt, rt) =>
        val result = q.execute.asInstanceOf[Iterable[(Any, Any)]]
        doFillExcelSheet(result, sheet, hasHead, targetTimeZone)(lt.cast[Any], rt.cast[Any])
      case t =>
        doFillExcelSheet(q.execute, sheet, hasHead, targetTimeZone)(t.cast[Any])
    }
  }

  val IterableClass = classOf[Iterable[_]]

  private def doFillExcelSheet[LeftType: TypeInfo, RightType: TypeInfo](
      results: Iterable[(LeftType, RightType)],
      sheet: Sheet,
      hasHeader: ExcelHeader,
      targetTimeZone: ZoneId): Unit =
    typeInfo[RightType] match {
      case TypeInfo(IterableClass +: _, _, _, Seq(inner)) =>
        ExcelHelper.fillSheetWithGroupResult(
          results.asInstanceOf[Iterable[(LeftType, Iterable[Any])]].toMap,
          sheet,
          hasHeader,
          targetTimeZone)(inner.cast[Any])
      case _ => ExcelHelper.fillSheetWithTupleResult(results, sheet, hasHeader, targetTimeZone)
    }

  private def doFillExcelSheet[RowType: TypeInfo](
      results: Iterable[RowType],
      sheet: Sheet,
      hasHeader: ExcelHeader,
      targetTimeZone: ZoneId): Unit =
    if (!results.isEmpty) typeInfo[RowType] match {
      case TupleTypeInfo(k, TypeInfo(_, _, _, v +: _)) =>
        ExcelHelper.fillSheetWithGroupResult(
          results.asInstanceOf[Iterable[(Any, Iterable[Any])]].toMap,
          sheet,
          hasHeader,
          targetTimeZone)
      case x => ExcelHelper.fillSheetWithNormalResult(results, sheet, hasHeader, targetTimeZone)
    }

  // ----------------------------------------------------------------------------------------------------------------------
  // ---------------------------------------- PivotTable Execution API           --------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------------
  @async def execute[LeftType, RowType <: DynamicObject, SrcType](
      pivot: PivotTable[LeftType, RowType, SrcType]): Iterable[RowType] = PivotHelper.executePivotTable(pivot)
  @async def display[LeftType, RowType <: DynamicObject, SrcType](pivot: PivotTable[LeftType, RowType, SrcType]): Unit =
    PivotHelper.displayPivotTable(pivot, System.out, None)
  @async def displayTo[LeftType, RowType <: DynamicObject, SrcType](
      pivot: PivotTable[LeftType, RowType, SrcType],
      os: OutputStream,
      timeZone: Option[ZoneId]): Unit = PivotHelper.displayPivotTable(pivot, os, timeZone)

  // ----------------------------------------------------------------------------------------------------------------------
  // ----------------------------------------     QueryProxy Execution API     --------------------------------------------
  // ----------------------------------------------------------------------------------------------------------------------
  @async def execute[T, K <: RelationKey[T]](proxy: QueryProxy[T, K]): Iterable[T] = execute(proxy.query)
}

object priqlTracePlugin extends SchedulerPlugin {
  override val pluginType: PluginType = PluginType("PRIQLTrace")
  override def adapt(n: NodeTask, ec: OGSchedulerContext): Boolean = {
    // Try to avoid a slow partial-function here.
    val args = n.args()
    var i = 0
    while (i < args.length) {
      val a = args(i)
      if (a.isInstanceOf[MethodPosition]) {
        val pos = a.asInstanceOf[MethodPosition]
        if (pos ne MethodPosition.unknown) {
          val info = pos.posInfo
          val linkFormat = CallerDetail.parser.matcher(info).replaceAll("(${file}:${line})")
          n.replace(n.scenarioStack().withPluginTag(CallerDetailKey, new CallerDetail(linkFormat, info)))
        }
        return false
      } else i = i + 1
    }
    false
  }
}

abstract class QueryProxy[T, K <: RelationKey[T]](val query: Query[T]) {
  def key: K = query.element.key.asInstanceOf[K]

  def shapeTypeTag: TypeInfo[T] = query.elementType.cast[T]
}
