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
package optimus.platform.relational

import optimus.platform._
import optimus.platform._
import optimus.platform.relational.aggregation._
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.relational.tree.MethodPosition

final case class QueryOps[T](src: Query[T]) {
  def sortBy[U](
      f: Lambda1[T, U])(implicit ordering: Ordering[U], sortType: TypeInfo[U], pos: MethodPosition): Query[T] = {
    src.provider.sortBy[T, U](src, f)(ordering, sortType, pos)
  }

  def distinct(implicit pos: MethodPosition): Query[T] = {
    src.provider.distinct(src)(pos)
  }

  def map[U](f: Lambda1[T, U])(implicit itemType: TypeInfo[U], pos: MethodPosition): Query[U] = {
    src.provider.map(src, f)
  }

  def flatMap[U, F[_]](f: Lambda1[T, F[U]])(implicit
      conv: QueryConverter[U, F],
      resultType: TypeInfo[U],
      pos: MethodPosition): Query[U] = {
    src.provider.flatMap(src, f)
  }

  def filter(p: Lambda1[T, Boolean])(implicit pos: MethodPosition): Query[T] = {
    src.provider.filter(src, p)
  }

  def groupBy[U](f: Lambda1[T, U])(implicit keyType: TypeInfo[U], pos: MethodPosition): GroupQuery[U, Query[T]] = {
    src.provider.groupBy(src, f)
  }

  def aggregateByExplicit[GroupKeyType, GroupValueType, AggregateType](
      f: Lambda1[Query[GroupValueType], AggregateType])(implicit
      groupKeyType: TypeInfo[GroupKeyType],
      groupValueType: TypeInfo[GroupValueType],
      aggregateType: TypeInfo[AggregateType],
      pos: MethodPosition): Query[GroupKeyType with AggregateType] = {
    src.provider.aggregateBy(src, f)(groupKeyType, groupValueType, aggregateType, pos)
  }

  def aggregateByImplicit[GroupKeyType, GroupValueType, AggregateType](
      aggregatorNodeFuncs: List[NodeFunction1[_, _]],
      aggregateTypes: List[TypeInfo[_]])(implicit
      groupKeyType: TypeInfo[GroupKeyType],
      groupValueType: TypeInfo[GroupValueType],
      aggregateType: TypeInfo[AggregateType],
      pos: MethodPosition): Query[GroupKeyType with AggregateType] = {
    val r = aggregatorNodeFuncs zip aggregateTypes
    src.provider.aggregateBy[T, GroupKeyType, GroupValueType, AggregateType](src, r)
  }

  def extend[U](field: String, f: Lambda1[DynamicObject, U])(implicit
      fieldType: TypeInfo[U],
      pos: MethodPosition): Query[DynamicObject] = {
    src.provider.extend(src.untype, field, f)
  }

  def shapeTo[U](f: Lambda1[DynamicObject, U])(implicit shapeToType: TypeInfo[U], pos: MethodPosition): Query[U] = {
    src.provider.shapeTo(src.untype, f)
  }

  def extendTyped[U](f: Lambda1[T, U])(implicit extensionType: TypeInfo[U], pos: MethodPosition): Query[T with U] = {
    src.provider.extendTyped(src, f)
  }

  def extendTypedValue[U](value: U)(implicit extensionType: TypeInfo[U], pos: MethodPosition): Query[T with U] = {
    src.provider.extendTypedValue[T, U](src, value)
  }

  def replace[U >: T](f: Lambda1[T, U])(implicit replaceType: TypeInfo[U], pos: MethodPosition): Query[T] = {
    src.provider.replace(src, f)
  }

  def replaceValue[U >: T](repVal: U)(implicit replaceType: TypeInfo[U], pos: MethodPosition): Query[T] = {
    src.provider.replaceValue(src, repVal)
  }

  @async def reduce[U >: T](op: Lambda2[U, U, U]): U = {
    src.provider.reduce(src, op)
  }

  @async def foldLeft[U](z: U)(op: Lambda2[U, T, U]): U = {
    src.provider.foldLeft(src, z)(op)
  }

  @async def aggregate[U >: T](aggregator: Aggregator[U]): aggregator.Result = {
    src.provider.aggregate[U](src, aggregator)
  }

  @async def aggregate[U](f: Lambda1[T, U], aggregator: Aggregator[U])(implicit
      aggregateType: TypeInfo[U]): aggregator.Result = {
    src.provider.aggregate[T, U](src, f, aggregator)(aggregateType)
  }

  @async def sum[U](f: Lambda1[T, U])(implicit sum: Sum[U]): U = {
    src.provider.sum(src, f)(sum)
  }

  @async def sum(implicit sum: Sum[T]): T = {
    src.provider.sum(src)(sum)
  }

  @async def average[U](f: Lambda1[T, U])(implicit avg: Avg[U]): avg.Result = {
    src.provider.average[T, U](src, f)(avg)
  }

  @async def average[U >: T](implicit avg: Avg[U]): avg.Result = {
    src.provider.average[U](src)(avg)
  }

  @async def variance[U](f: Lambda1[T, U])(implicit v: Variance[U]): v.Result = {
    src.provider.variance[T, U](src, f)(v)
  }

  @async def variance[U >: T](implicit v: Variance[U]): v.Result = {
    src.provider.variance[U](src)(v)
  }

  @async def stddev[U](f: Lambda1[T, U])(implicit stdDev: StandardDeviation[U]): stdDev.Result = {
    src.provider.stddev[T, U](src, f)(stdDev)
  }

  @async def stddev[U >: T](implicit stdDev: StandardDeviation[U]): stdDev.Result = {
    src.provider.stddev[U](src)(stdDev)
  }

  @async def corr(x: Lambda1[T, Double], y: Lambda1[T, Double])(implicit cor: Correlation[Double]): Double = {
    src.provider.corr(src, x, y)(cor)
  }

  @async def min[U](f: Lambda1[T, U])(implicit min: Min[U]): U = {
    src.provider.min(src, f)(min)
  }

  @async def min(implicit min: Min[T]): T = {
    src.provider.min(src)(min)
  }

  @async def max[U](f: Lambda1[T, U])(implicit max: Max[U]): U = {
    src.provider.max(src, f)(max)
  }

  @async def max(implicit max: Max[T]): T = {
    src.provider.max(src)(max)
  }

  @async def count(f: Lambda1[T, Boolean]): Long = {
    src.provider.count(src, f)
  }

  @async def count: Long = {
    src.provider.count(src)
  }

  @async def head: T = {
    src.provider.head(src)
  }

  @async def headOption: Option[T] = {
    src.provider.headOption(src)
  }

  @async def last: T = {
    src.provider.last(src)
  }

  @async def lastOption: Option[T] = {
    src.provider.lastOption(src)
  }
}

final case class JoinQueryOps[L, R](val src: JoinQuery[L, R]) {
  def on(
      f: Lambda2[L, R, Boolean],
      fLeft: Option[Lambda1[L, MultiKey]] = None,
      fRight: Option[Lambda1[R, MultiKey]] = None): JoinQuery[L, R] = {
    src.provider.on(src, f, fLeft, fRight)
  }

  def withLeftDefault[B >: L](f: Lambda1[R, B])(implicit b: TypeInfo[B], pos: MethodPosition): JoinQuery[B, R] = {
    src.provider.withLeftDefault(src, f)
  }

  def withRightDefault[B >: R](f: Lambda1[L, B])(implicit b: TypeInfo[B], pos: MethodPosition): JoinQuery[L, B] = {
    src.provider.withRightDefault(src, f)
  }

  def onNatural: NaturalJoinQuery[L, R] = {
    src.provider.onNatural(src)
  }
}

final case class NaturalJoinQueryOps[L, R](val src: NaturalJoinQuery[L, R]) {
  def withLeftDefaultNatural[B >: L](
      f: Lambda1[R, B])(implicit b: TypeInfo[B], pos: MethodPosition): NaturalJoinQuery[B, R] = {
    src.provider.withLeftDefaultNatural(src, f)
  }

  def withRightDefaultNatural[B >: R](
      f: Lambda1[L, B])(implicit b: TypeInfo[B], pos: MethodPosition): NaturalJoinQuery[L, B] = {
    src.provider.withRightDefaultNatural(src, f)
  }
}

final case class GroupQueryOps[T, U](val src: GroupQuery[T, U]) {
  def mapValues[S](f: Lambda1[U, S])(implicit resultType: TypeInfo[S], pos: MethodPosition): GroupQuery[T, S] = {
    src.provider.mapValues(src, f)
  }
}
