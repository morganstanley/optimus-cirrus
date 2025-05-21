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

import optimus.platform.relational.DefaultQueryProvider
import optimus.platform.relational.KeyPropagationPolicy
import optimus.graph.Node
import optimus.platform._
import optimus.platform.annotations.assumeParallelizableInClosure
import optimus.platform.asNodeInnards.NodeFunction1Impl
import optimus.platform.asNodeInnards.NodeFunction2Impl
import optimus.platform.relational.aggregation._
import optimus.platform.relational.internal.OptimusCoreAPI
import optimus.platform.relational.tree._

import scala.util.Try

/**
 * Provides the real implementation of the standard PRIQL operators for querying data sources that implement 'Query[T]'
 */
trait QueryProvider extends Serializable {

  /**
   * Permit table scan, most data sources will ignore this operator except DAL/DB
   */
  def permitTableScan[T](src: Query[T])(implicit pos: MethodPosition): Query[T]

  /**
   * @see
   *   [[optimus.platform.Query.entitledOnly]]
   */
  def entitledOnly[T](src: Query[T])(implicit pos: MethodPosition): Query[T]

  /**
   * Projects each item of a relation into a new form using the selector function 'f'
   */
  def map[T, U: TypeInfo](src: Query[T], f: Lambda1[T, U])(implicit pos: MethodPosition): Query[U]

  /**
   * Projects each item of a relation to an 'F[U]' which could be further converted to relation using 'conv' and
   * combines the resulting relations into one relation
   */
  def flatMap[T, U, F[_]](src: Query[T], f: Lambda1[T, F[U]])(implicit
      conv: QueryConverter[U, F],
      resultType: TypeInfo[U],
      pos: MethodPosition): Query[U]

  /**
   * Filters a relation of items based on predicator 'p'
   */
  def filter[T](src: Query[T], p: Lambda1[T, Boolean])(implicit pos: MethodPosition): Query[T]

  /**
   * Groups the items of a relation according to a specified key selector function 'f'.
   */
  def groupBy[T, U: TypeInfo](src: Query[T], f: Lambda1[T, U])(implicit pos: MethodPosition): GroupQuery[U, Query[T]]

  /**
   * Groups the items of a relation according to specified type GroupKeyType
   */
  def groupBy[T, GroupKeyType: TypeInfo, GroupValueType: TypeInfo](src: Query[T])(implicit
      pos: MethodPosition): GroupQuery[GroupKeyType, Query[GroupValueType]]

  /**
   * First group by GroupKeyType, then apply f to every sub group Query[GroupValueType] to generate one AggregateType
   * value
   */
  def aggregateBy[T, GroupKeyType: TypeInfo, GroupValueType: TypeInfo, AggregateType: TypeInfo](
      src: Query[T],
      f: Lambda1[Query[GroupValueType], AggregateType])(implicit
      pos: MethodPosition): Query[GroupKeyType with AggregateType]

  /**
   * First group by GroupKeyType, then apply "aggregatorNodeFuncs" to every sub group of GroupValueType to generate one
   * AggregateType value
   */
  def aggregateBy[T, GroupKeyType: TypeInfo, GroupValueType: TypeInfo, AggregateType: TypeInfo](
      src: Query[T],
      aggregators: List[(NodeFunction1[_, _], TypeInfo[_])])(implicit
      pos: MethodPosition): Query[GroupKeyType with AggregateType]

  /**
   * Aggregate the untyped items: first group them according to groupByParameters then using provided Aggregator to
   * generate a Dynamic object which contains both group value and aggregate value
   */
  def aggregateByUntyped(
      src: Query[DynamicObject],
      groupByProperties: Set[String],
      untypedAggregations: UntypedAggregation*)(implicit pos: MethodPosition): Query[DynamicObject]

  /**
   * Transforms this group relation by applying 'f' to every value.
   */
  def mapValues[T, U, S: TypeInfo](src: GroupQuery[T, U], f: Lambda1[U, S])(implicit
      pos: MethodPosition): GroupQuery[T, S]

  /**
   * Inner joins the elements of two relations based on the following 'on' lambda (or using the default keys in both
   * relations)
   */
  def innerJoin[L, R](left: Query[L], right: Query[R])(implicit pos: MethodPosition): JoinQuery[L, R]

  /**
   * Left outer joins the elements of two relations based on the following 'on' lambda (or using the default keys in
   * both relations). The right default value is either 'null' or provided by following 'withRightDefault' method
   */
  def leftOuterJoin[L, R](left: Query[L], right: Query[R])(implicit pos: MethodPosition): JoinQuery[L, R]

  /**
   * Right outer joins the elements of two relations based on the following 'on' lambda (or using the default keys in
   * both relations). The left default value is either 'null' or provided by following 'withLeftDefault' method
   */
  def rightOuterJoin[L, R](left: Query[L], right: Query[R])(implicit pos: MethodPosition): JoinQuery[L, R]

  /**
   * Full outer joins the elements of two relations based on the following 'on' lambda (or using the default keys in
   * both relations). The right default value is either 'null' or provided by following 'withRightDefault' method. The
   * left default value is either 'null' or provided by following 'withLeftDefault' method
   */
  def fullOuterJoin[L, R](left: Query[L], right: Query[R])(implicit pos: MethodPosition): JoinQuery[L, R]

  /**
   * Defines the join condition. fLeft and fRight are for performance enhancement
   */
  def on[L, R](
      src: JoinQuery[L, R],
      f: Lambda2[L, R, Boolean],
      fLeft: Option[Lambda1[L, MultiKey]] = None,
      fRight: Option[Lambda1[R, MultiKey]] = None): JoinQuery[L, R]

  def on[L, R](src: JoinQuery[L, R], fields: Seq[String]): JoinQuery[L, R]

  def onNatural[L, R](src: JoinQuery[L, R])(implicit pos: MethodPosition): NaturalJoinQuery[L, R]

  def withLeftDefault[L, R, B >: L](src: JoinQuery[L, R], f: Lambda1[R, B])(implicit
      b: TypeInfo[B],
      pos: MethodPosition): JoinQuery[B, R]

  def withRightDefault[L, R, B >: R](src: JoinQuery[L, R], f: Lambda1[L, B])(implicit
      b: TypeInfo[B],
      pos: MethodPosition): JoinQuery[L, B]

  def withLeftDefaultNatural[L, R, B >: L](src: NaturalJoinQuery[L, R], f: Lambda1[R, B])(implicit
      b: TypeInfo[B],
      pos: MethodPosition): NaturalJoinQuery[B, R]

  def withRightDefaultNatural[L, R, B >: R](src: NaturalJoinQuery[L, R], f: Lambda1[L, B])(implicit
      b: TypeInfo[B],
      pos: MethodPosition): NaturalJoinQuery[L, B]

  /**
   * Produces the set union of multiple relations by using the default equality-comparer/relation-key
   */
  def union[T](src: Query[T], others: Query[T]*)(implicit pos: MethodPosition): Query[T]

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
  def merge[T, V >: T](src: Query[T], others: Query[T]*)(ag: Aggregator[V] { type Result = V })(implicit
      pos: MethodPosition): Query[T]

  /**
   * The difference operator will pick up all elements in current relation but not in the 'other' relation
   */
  def difference[T](src: Query[T], other: Query[T])(implicit pos: MethodPosition): Query[T]

  /**
   * Turns a strong typed relation into dynamic typed relation
   */
  def untype[T](src: Query[T])(implicit pos: MethodPosition): Query[DynamicObject]

  /**
   * The extend operator will convert the relation to a dynamic type and extend it.
   */
  def extend[U](src: Query[DynamicObject], field: String, f: Lambda1[DynamicObject, U])(implicit
      fieldType: TypeInfo[U],
      pos: MethodPosition): Query[DynamicObject]

  /**
   * The shapeTo operator will shape one relation to another type and at the same time retain the columns of the
   * original type.
   */
  def shapeTo[U](src: Query[DynamicObject], f: Lambda1[DynamicObject, U])(implicit
      shapeToType: TypeInfo[U],
      pos: MethodPosition): Query[U]

  /**
   * The extendTyped operator will en-rich the current relation of type T with an extra coordinates of type U, the
   * result becomes a relation of type T with U
   */
  def extendTyped[T, U: TypeInfo](src: Query[T], f: Lambda1[T, U])(implicit pos: MethodPosition): Query[T with U]

  /**
   * Same as extendTyped
   */
  def extendTypedValue[T, U: TypeInfo](src: Query[T], extVal: U)(implicit pos: MethodPosition): Query[T with U]

  /**
   * The replace operator will replace the value of existing coordinates in the relation.
   */
  def replace[T, U >: T: TypeInfo](src: Query[T], f: Lambda1[T, U])(implicit pos: MethodPosition): Query[T]

  def replaceValue[T, U >: T: TypeInfo](src: Query[T], repVal: U)(implicit pos: MethodPosition): Query[T]

  def extendTypedOrReplaceValue[T, U: TypeInfo](src: Query[T], value: U)(implicit pos: MethodPosition): Query[T with U]

  /**
   * The sortBy operator will sort row based on user specified fields
   */
  def sortBy[T, U](src: Query[T], f: Lambda1[T, U])(implicit
      ordering: Ordering[U],
      sortType: TypeInfo[U],
      pos: MethodPosition): Query[T]

  /**
   * A newly added operator, it could guarantee the absolute order among rows.
   *
   * So if two rows have same sorted key values, their relative order could be decided by this operator. If we call take
   * operator later, RT could be guaranteed too. There are some rules to guarantee absolute order:
   *
   * 1 a source itself could be guaranteed by nature e.g Seq in memory otherwise
   *
   * 2 sort on priql key
   */
  def arrange[T](src: Query[T])(implicit pos: MethodPosition): Query[T]

  /**
   * Returns distinct elements from a sequence by using the default equality comparer
   */
  def distinct[T](src: Query[T])(implicit pos: MethodPosition): Query[T]

  @async def reduce[T, U >: T](src: Query[T], op: Lambda2[U, U, U]): U

  @async def foldLeft[T, U](src: Query[T], z: U)(op: Lambda2[U, T, U]): U

  @async def aggregate[T](src: Query[T], aggregator: Aggregator[T]): aggregator.Result

  @async def aggregate[T, U](src: Query[T], f: Lambda1[T, U], aggregator: Aggregator[U])(implicit
      aggregateType: TypeInfo[U]): aggregator.Result

  @async def sum[T](src: Query[T])(implicit sum: Sum[T]): T

  @async def sum[T, U](src: Query[T], f: Lambda1[T, U])(implicit sum: Sum[U]): U

  @async def average[T](src: Query[T])(implicit avg: Avg[T]): avg.Result

  @async def average[T, U](src: Query[T], f: Lambda1[T, U])(implicit avg: Avg[U]): avg.Result

  @async def variance[T](src: Query[T])(implicit v: Variance[T]): v.Result

  @async def variance[T, U](src: Query[T], f: Lambda1[T, U])(implicit v: Variance[U]): v.Result

  @async def stddev[T](src: Query[T])(implicit stdDev: StandardDeviation[T]): stdDev.Result

  @async def stddev[T, U](src: Query[T], f: Lambda1[T, U])(implicit stdDev: StandardDeviation[U]): stdDev.Result

  @async def corr[T](src: Query[T], x: Lambda1[T, Double], y: Lambda1[T, Double])(implicit
      cor: Correlation[Double]): Double

  @async def min[T](src: Query[T])(implicit min: Min[T]): T

  @async def min[T, U](src: Query[T], f: Lambda1[T, U])(implicit min: Min[U]): U

  @async def max[T](src: Query[T])(implicit max: Max[T]): T

  @async def max[T, U](src: Query[T], f: Lambda1[T, U])(implicit max: Max[U]): U

  @async def count[T](src: Query[T]): Long

  @async def count[T](src: Query[T], f: Lambda1[T, Boolean]): Long

  @async def head[T](src: Query[T]): T

  @async def headOption[T](src: Query[T]): Option[T]

  @async def last[T](src: Query[T]): T

  @async def lastOption[T](src: Query[T]): Option[T]

  /**
   * The take operator will take num rows starting from offset
   */
  def take[T](src: Query[T], offset: Int, num: Int)(implicit pos: MethodPosition): Query[T]

  /**
   * Iterable based execution, return a compiled lambda instead of directly executing it
   */
  def execute[T](e: RelationElement, executeOptions: ExecuteOptions = ExecuteOptions.Default): NodeFunction0[T]

  /**
   * create a Query instance
   */
  def createQuery[T](e: MultiRelationElement): Query[T]
}

object QueryProvider {
  case object EntityAndDimensionOnly extends DefaultQueryProvider with KeyPropagationPolicy.EntityAndDimensionOnly

  case object Legacy extends DefaultQueryProvider with KeyPropagationPolicy.Legacy {
    override def sortBy[T, U](src: Query[T], f: Lambda1[T, U])(implicit
        ordering: Ordering[U],
        sortType: TypeInfo[U],
        pos: MethodPosition): Query[T] = {
      import optimus.platform.{NoKey => NK}
      val sourceType = src.elementType
      val argument = new Argument(sourceType)

      val lambda = f match {
        case Lambda1(Some(f), _, lambdaFunc) =>
          new FuncElement(new ScalaLambdaCallee(Right(f), lambdaFunc, sortType, argument), Nil, null)
        case Lambda1(_, Some(nf), lambdaFunc) =>
          new FuncElement(new ScalaLambdaCallee(Left(nf), lambdaFunc, sortType, argument), Nil, null)
        case Lambda1(None, None, _) =>
          throw new IllegalArgumentException("expected either a Function or a NodeFunction but both were None")
      }
      val method = new MethodElement(
        QueryMethod.SORT,
        List(
          MethodArg(MethodArgConstants.source, src.element),
          MethodArg(MethodArgConstants.function, lambda),
          MethodArg("ordering", new ConstValueElement(ordering))),
        sourceType,
        NK,
        pos
      )
      QueryProvider.NoKey.createQuery[T](method)
    }
  }

  case object NoKey extends DefaultQueryProvider with KeyPropagationPolicy.NoKey
}

/**
 * PRIQL version of Function1. If the original lambda has no async call, it will be captured into 'func', otherwise it
 * will be in 'nodeFunc'.
 */
final case class Lambda1[-T1, +R] private (
    func: Option[T1 => R],
    nodeFunc: Option[T1 => Node[R]],
    lambda: Option[() => Try[LambdaElement]] = None) {
  def hasNodeFunc: Boolean = nodeFunc.isDefined
  def hasFunc: Boolean = func.isDefined
  def hasLambda: Boolean = lambda.isDefined
}
object Lambda1 {
  def create[T1, R](
      @assumeParallelizableInClosure f: T1 => R,
      lambda: Option[() => Try[LambdaElement]]): Lambda1[T1, R] = {
    Lambda1(Some(f), None, lambda)
  }

  def create[T1, R](
      @assumeParallelizableInClosure f: NodeFunction1[T1, R],
      lambda: Option[() => Try[LambdaElement]]): Lambda1[T1, R] = {
    val nf = f match {
      case x: NodeFunction1Impl[T1, R] => x.vn
      case _                           => OptimusCoreAPI.liftNode((t: T1) => f(t))
    }
    Lambda1(None, Some(nf), lambda)
  }
}

/**
 * PRIQL version of Function2. If the original lambda has no async call, it will be captured into 'func', otherwise it
 * will be in 'nodeFunc'.
 */
final case class Lambda2[-T1, -T2, +R](
    func: Option[(T1, T2) => R],
    nodeFunc: Option[(T1, T2) => Node[R]],
    lambda: Option[() => Try[LambdaElement]] = None) {
  def hasNodeFunc: Boolean = nodeFunc.isDefined
  def hasFunc: Boolean = func.isDefined
  def hasLambda: Boolean = lambda.isDefined
}
object Lambda2 {
  def create[T1, T2, R](
      @assumeParallelizableInClosure f: (T1, T2) => R,
      lambda: Option[() => Try[LambdaElement]]): Lambda2[T1, T2, R] = {
    Lambda2(Some(f), None, lambda)
  }

  def create[T1, T2, R](
      @assumeParallelizableInClosure f: NodeFunction2[T1, T2, R],
      lambda: Option[() => Try[LambdaElement]]): Lambda2[T1, T2, R] = {
    val nf = f match {
      case x: NodeFunction2Impl[T1, T2, R] => x.vn
      case _                               => OptimusCoreAPI.liftNode((t1: T1, t2: T2) => f(t1, t2))
    }
    Lambda2(None, Some(nf), lambda)
  }
}
