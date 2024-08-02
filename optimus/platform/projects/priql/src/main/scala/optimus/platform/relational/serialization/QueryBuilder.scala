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
package optimus.platform.relational.serialization

import optimus.platform._
import optimus.platform._
import optimus.platform.relational.QueryOps
import optimus.platform.relational._
import optimus.platform.relational.aggregation.Aggregator
import optimus.platform.relational.aggregation.UntypedAggregation
import optimus.platform.relational.inmemory.JoinHelper
import optimus.platform.relational.tree.MethodArgConstants.rightDefault
import optimus.platform.relational.tree._

trait QueryBuilderConvertible {
  def toQueryBuilder: QueryBuilder[_]
}

/**
 * QueryBuilder is a serializable representation of a PriQL Query. In principle, classes that implement this trait
 * capture the serializable params of the corresponding Query API / QueryOps methods. Each QueryBuilder instance can be
 * converted back to the corresponding Query via a call to `.build`.
 */
trait QueryBuilder[T] extends Serializable {
  def build: Query[T]
}

trait JoinQueryBuilder[L, R] extends QueryBuilder[(L, R)] {
  def build: JoinQuery[L, R]
}

trait NaturalJoinQueryBuilder[L, R] extends QueryBuilder[(L with R)] {
  def build: NaturalJoinQuery[L, R]
}

trait GroupQueryBuilder[T, U] extends QueryBuilder[(T, U)] {
  def build: GroupQuery[T, U]
}

final case class InMemoryFrom[T](
    data: Iterable[T],
    itemType: TypeInfo[T],
    shouldExecuteDistinct: Boolean,
    keyPolicy: KeyPropagationPolicy,
    pos: MethodPosition
) extends QueryBuilder[T] {

  protected def writeReplace: Object = {
    val annonOpt = itemType.classes
      .find(c => c.isAnnotationPresent(classOf[collectionSerializer]))
      .map(_.getAnnotation(classOf[collectionSerializer]))
    annonOpt
      .map { annon =>
        val serializer = annon.serializer.getDeclaredConstructor().newInstance()
        InMemoryFrom.Custom(
          serializer.writeReplace(data, itemType),
          itemType,
          shouldExecuteDistinct,
          keyPolicy,
          pos
        )
      }
      .getOrElse {
        InMemoryFrom.Normal(
          data,
          itemType,
          shouldExecuteDistinct,
          keyPolicy,
          pos
        )
      }
  }

  def build: Query[T] = {
    val distinctAdjustedData = if (shouldExecuteDistinct) data else data.toSet
    from(distinctAdjustedData, keyPolicy)(implicitly[QueryConverter[T, Iterable]], itemType, pos)
  }
}

object InMemoryFrom {
  // normal java serialization
  final case class Normal[T](
      data: Iterable[T],
      itemType: TypeInfo[T],
      shouldExecuteDistinct: Boolean,
      keyPolicy: KeyPropagationPolicy,
      pos: MethodPosition
  ) {
    def readResolve: Object =
      InMemoryFrom(data, itemType, shouldExecuteDistinct, keyPolicy, pos)
  }
  // Custom relation serialization (for examples see implementors of @collectionSerializer annotation)
  final case class Custom[T](
      data: Object,
      itemType: TypeInfo[T],
      shouldExecuteDistinct: Boolean,
      keyPolicy: KeyPropagationPolicy,
      pos: MethodPosition
  ) {
    def readResolve: Object =
      // the data will be readResolve as Iterable during deserialization
      InMemoryFrom(data.asInstanceOf[Iterable[T]], itemType, shouldExecuteDistinct, keyPolicy, pos)
  }
}

final case class LambdaFrom[T](
    lambda: () => Query[T]
) extends QueryBuilder[T] {
  def build: Query[T] = {
    lambda()
  }
}

final case class Filter[T](
    src: QueryBuilder[T],
    predicate: Lambda1[T, Boolean],
    pos: MethodPosition
) extends QueryBuilder[T] {
  def build: Query[T] = {
    QueryOps(src.build).filter(predicate)(pos)
  }
}

final case class Select[T, U](
    src: QueryBuilder[T],
    f: Lambda1[T, U],
    itemType: TypeInfo[U],
    pos: MethodPosition
) extends QueryBuilder[U] {
  def build: Query[U] = {
    QueryOps(src.build).map(f)(itemType, pos)
  }
}

final case class FlatMap[T, U, F[_]](
    src: QueryBuilder[T],
    f: Lambda1[T, F[U]],
    conv: QueryConverter[U, F],
    resultType: TypeInfo[U],
    pos: MethodPosition
) extends QueryBuilder[U] {
  def build: Query[U] = {
    QueryOps(src.build).flatMap(f)(conv, resultType, pos)
  }
}

final case class GroupBy[T, U](
    src: QueryBuilder[T],
    f: Lambda1[T, U],
    keyType: TypeInfo[U],
    pos: MethodPosition
) extends GroupQueryBuilder[U, Query[T]] {
  def build: GroupQuery[U, Query[T]] = {
    QueryOps(src.build).groupBy(f)(keyType, pos)
  }
}

final case class MapValues[T, U, S](
    src: GroupQueryBuilder[T, U],
    f: Lambda1[U, S],
    resultType: TypeInfo[S],
    pos: MethodPosition
) extends GroupQueryBuilder[T, S] {
  def build: GroupQuery[T, S] = {
    GroupQueryOps(src.build).mapValues(f)(resultType, pos)
  }
}

final case class Extend[U](
    src: QueryBuilder[_],
    field: String,
    f: Lambda1[DynamicObject, U],
    fieldType: TypeInfo[U],
    pos: MethodPosition
) extends QueryBuilder[DynamicObject] {
  def build: Query[DynamicObject] = {
    QueryOps(src.build).extend(field, f)(fieldType, pos)
  }
}

final case class ExtendTyped[T, U](
    src: QueryBuilder[T],
    f: Lambda1[T, U],
    extensionType: TypeInfo[U],
    pos: MethodPosition
) extends QueryBuilder[T with U] {
  def build: Query[T with U] = {
    QueryOps(src.build).extendTyped(f)(extensionType, pos)
  }
}

final case class ExtendTypedValue[T, U](
    src: QueryBuilder[T],
    extensionValue: U,
    extensionType: TypeInfo[U],
    pos: MethodPosition
) extends QueryBuilder[T with U] {
  def build: Query[T with U] = {
    QueryOps(src.build).extendTypedValue(extensionValue)(extensionType, pos)
  }
}

final case class Replace[T, U >: T](
    src: QueryBuilder[T],
    f: Lambda1[T, U],
    replaceType: TypeInfo[U],
    pos: MethodPosition
) extends QueryBuilder[T] {
  def build: Query[T] = {
    QueryOps(src.build).replace(f)(replaceType, pos)
  }
}

final case class ReplaceValue[T, U >: T](
    src: QueryBuilder[T],
    replaceValue: U,
    replaceType: TypeInfo[U],
    pos: MethodPosition
) extends QueryBuilder[T] {
  def build: Query[T] = {
    QueryOps(src.build).replaceValue(replaceValue)(replaceType, pos)
  }
}

final case class Union[T, U >: T](
    src: QueryBuilder[T],
    others: Seq[QueryBuilder[U]],
    pos: MethodPosition
) extends QueryBuilder[U] {
  def build: Query[U] = {
    src.build.union(others.map(_.build): _*)
  }
}

final case class Merge[T, U >: T, V >: U](
    src: QueryBuilder[T],
    others: Seq[QueryBuilder[U]],
    ag: Aggregator[V] { type Result = V },
    pos: MethodPosition
) extends QueryBuilder[U] {
  def build: Query[U] = {
    src.build.merge[U, V](others.map(_.build): _*)(ag)(pos)
  }
}

final case class Difference[T, U >: T](
    src: QueryBuilder[T],
    other: QueryBuilder[U],
    pos: MethodPosition
) extends QueryBuilder[U] {
  def build: Query[U] = {
    src.build.difference(other.build)(pos)
  }
}

final case class Sort[T, U](
    src: QueryBuilder[T],
    f: Lambda1[T, U],
    ordering: Ordering[U],
    sortType: TypeInfo[U],
    pos: MethodPosition
) extends QueryBuilder[T] {
  def build: Query[T] = {
    QueryOps(src.build).sortBy(f)(ordering, sortType, pos)
  }
}

final case class Arrange[T](
    src: QueryBuilder[T],
    pos: MethodPosition
) extends QueryBuilder[T] {
  def build: Query[T] = {
    src.build.arrange(pos)
  }
}

final case class Distinct[T](
    src: QueryBuilder[T],
    pos: MethodPosition
) extends QueryBuilder[T] {
  def build: Query[T] = {
    src.build.distinct(pos)
  }
}

final case class Take[T](
    src: QueryBuilder[T],
    offset: Int,
    rowCount: Int,
    pos: MethodPosition
) extends QueryBuilder[T] {
  def build: Query[T] = {
    src.build.take(offset, rowCount)(pos)
  }
}

final case class Untype(
    src: QueryBuilder[_],
    pos: MethodPosition
) extends QueryBuilder[DynamicObject] {
  def build: Query[DynamicObject] = {
    src.build.untype(pos)
  }
}

final case class ShapeTo[U](
    src: QueryBuilder[_],
    f: Lambda1[DynamicObject, U],
    shapeToType: TypeInfo[U],
    pos: MethodPosition
) extends QueryBuilder[U] {
  def build: Query[U] = {
    QueryOps(src.build).shapeTo(f)(shapeToType, pos)
  }
}

final case class InnerJoin[L, R](
    left: QueryBuilder[L],
    right: QueryBuilder[R],
    pos: MethodPosition
) extends JoinQueryBuilder[L, R] {
  def build: JoinQuery[L, R] = {
    left.build.innerJoin(right.build)(pos)
  }
}

final case class LeftOuterJoin[L, R](
    left: QueryBuilder[L],
    right: QueryBuilder[R],
    pos: MethodPosition
) extends JoinQueryBuilder[L, R] {
  def build: JoinQuery[L, R] = {
    left.build.leftOuterJoin(right.build)(pos)
  }
}

final case class RightOuterJoin[L, R](
    left: QueryBuilder[L],
    right: QueryBuilder[R],
    pos: MethodPosition
) extends JoinQueryBuilder[L, R] {
  def build: JoinQuery[L, R] = {
    left.build.rightOuterJoin(right.build)(pos)
  }
}

final case class FullOuterJoin[L, R](
    left: QueryBuilder[L],
    right: QueryBuilder[R],
    pos: MethodPosition
) extends JoinQueryBuilder[L, R] {
  def build: JoinQuery[L, R] = {
    left.build.fullOuterJoin(right.build)(pos)
  }
}

final case class JoinOnNatural[L, R](
    src: JoinQueryBuilder[L, R]
) extends NaturalJoinQueryBuilder[L, R] {
  def build: NaturalJoinQuery[L, R] = {
    JoinQueryOps(src.build).onNatural
  }
}

final case class JoinOnPredicate[L, R](
    src: JoinQueryBuilder[L, R],
    f: Lambda2[L, R, Boolean],
    fLeft: Option[Lambda1[L, MultiKey]] = None,
    fRight: Option[Lambda1[R, MultiKey]] = None
) extends JoinQueryBuilder[L, R] {
  def build: JoinQuery[L, R] = {
    JoinQueryOps(src.build).on(f, fLeft, fRight)
  }
}

final case class JoinOnFields[L, R](
    src: JoinQueryBuilder[L, R],
    fields: Seq[String]
) extends JoinQueryBuilder[L, R] {
  def build: JoinQuery[L, R] = {
    src.build.on(fields)
  }
}

final case class JoinWithLeftDefault[L, R, B >: L](
    src: JoinQueryBuilder[L, R],
    f: Lambda1[R, B],
    b: TypeInfo[B],
    pos: MethodPosition
) extends JoinQueryBuilder[B, R] {
  def build: JoinQuery[B, R] = {
    JoinQueryOps(src.build).withLeftDefault(f)(b, pos)
  }
}

final case class JoinWithRightDefault[L, R, B >: R](
    src: JoinQueryBuilder[L, R],
    f: Lambda1[L, B],
    b: TypeInfo[B],
    pos: MethodPosition
) extends JoinQueryBuilder[L, B] {
  def build: JoinQuery[L, B] = {
    JoinQueryOps(src.build).withRightDefault(f)(b, pos)
  }
}

final case class AggregateBy[GroupKeyType, GroupValueType, AggregateType](
    src: QueryBuilder[_],
    f: Lambda1[Query[GroupValueType], AggregateType],
    groupKeyType: TypeInfo[GroupKeyType],
    groupValueType: TypeInfo[GroupValueType],
    aggregateType: TypeInfo[AggregateType],
    pos: MethodPosition
) extends QueryBuilder[GroupKeyType with AggregateType] {
  def build: Query[GroupKeyType with AggregateType] = {
    QueryOps(src.build).aggregateByExplicit(f)(groupKeyType, groupValueType, aggregateType, pos)
  }
}

final case class AggregateByImplicit[T, GroupKeyType >: T, GroupValueType >: T, AggregateType](
    src: QueryBuilder[T],
    aggregatorNodeFuncs: List[NodeFunction1[_, _]],
    aggregateTypes: List[TypeInfo[_]],
    groupKeyType: TypeInfo[GroupKeyType],
    groupValueType: TypeInfo[GroupValueType],
    aggregateType: TypeInfo[AggregateType],
    pos: MethodPosition
) extends QueryBuilder[GroupKeyType with AggregateType] {
  def build: Query[GroupKeyType with AggregateType] = {
    QueryOps(src.build)
      .aggregateByImplicit(aggregatorNodeFuncs, aggregateTypes)(groupKeyType, groupValueType, aggregateType, pos)
  }
}

final case class AggregateByUntyped(
    src: QueryBuilder[_],
    groupByProperties: Set[String],
    untypedAggregations: Seq[UntypedAggregation],
    pos: MethodPosition
) extends QueryBuilder[DynamicObject] {
  def build: Query[DynamicObject] = {
    src.build.aggregateByUntyped(groupByProperties, untypedAggregations: _*)(pos)
  }
}

final case class PermitTableScan[T](
    src: QueryBuilder[T],
    pos: MethodPosition
) extends QueryBuilder[T] {
  def build: Query[T] = {
    src.build.permitTableScan(pos)
  }
}

object QueryBuilder {
  def of(e: RelationElement): QueryBuilder[_] = {
    e match {
      case null                                             => null
      case m: MethodElement                                 => of(m)
      case p: ProviderRelation with QueryBuilderConvertible => p.toQueryBuilder
      case _ => throw new IllegalArgumentException(s"Unsupported relation element: $e")
    }
  }

  def of(method: MethodElement): QueryBuilder[_] = {
    import QueryMethod._

    val apply: (MethodElement) => QueryBuilder[_] =
      method.methodCode match {
        case WHERE            => ofFilter
        case OUTPUT           => ofSelect
        case FLATMAP          => ofFlatmap
        case GROUP_BY         => ofGroupBy
        case GROUP_MAP_VALUES => ofMapValues
        case EXTEND           => ofExtend
        case EXTEND_TYPED     => ofExtendTyped
        case REPLACE          => ofReplace
        case UNION            => ofUnion
        case MERGE            => ofMerge
        case SORT             => ofSort
        case ARRANGE          => ofArrange
        case DIFFERENCE       => ofDifference
        case TAKE             => ofTake
        case TAKE_DISTINCT    => ofDistinct
        case UNTYPE           => ofUntype
        case SHAPE            => ofShape
        case INNER_JOIN | LEFT_OUTER_JOIN | RIGHT_OUTER_JOIN | FULL_OUTER_JOIN =>
          ofTupleJoin
        case NATURAL_INNER_JOIN | NATURAL_LEFT_OUTER_JOIN | NATURAL_RIGHT_OUTER_JOIN | NATURAL_FULL_OUTER_JOIN =>
          ofNaturalJoin
        case AGGREGATE_BY          => ofAggregateBy
        case AGGREGATE_BY_IMPLICIT => ofAggregateByImplicit
        case AGGREGATE_BY_UNTYPED  => ofAggregateByUntyped
        case PERMIT_TABLE_SCAN     => ofPermitTableScan
        case _                     => throw new IllegalArgumentException(s"Unsupported method element: $method")
      }

    apply(method)
  }

  private def ofFilter[T](method: MethodElement): QueryBuilder[T] = {
    val (
      src ::
        FuncElement(c: ScalaLambdaCallee[T, Boolean] @unchecked, _, _) ::
        _
    ) = takeParams(method, 2)

    Filter(of(src).asInstanceOf[QueryBuilder[T]], toLambda1(c), method.pos)
  }

  private def ofSelect[T, U](method: MethodElement): QueryBuilder[U] = {
    val (
      src ::
        FuncElement(c: ScalaLambdaCallee[T, U] @unchecked, _, _) ::
        _
    ) = takeParams(method, 2)

    Select(of(src).asInstanceOf[QueryBuilder[T]], toLambda1(c), c.resType, method.pos)
  }

  private def ofFlatmap[T, U, F[_]](method: MethodElement): QueryBuilder[U] = {
    val (
      src ::
        FuncElement(c: ScalaLambdaCallee[T, F[U]] @unchecked, _, _) ::
        ConstValueElement(conv: QueryConverter[U, F] @unchecked, _) ::
        _
    ) = takeParams(method, 3)

    // if the query converter conv has already been applied to the original lambda, we shouldn't apply it again
    val queryConverter =
      if (c.resType <:< classOf[Query[_]]) QueryConverter.selfConverter.asInstanceOf[QueryConverter[U, F]] else conv

    FlatMap(of(src).asInstanceOf[QueryBuilder[T]], toLambda1(c), queryConverter, method.rowTypeInfo.cast[U], method.pos)
  }

  private def ofGroupBy[T, U](method: MethodElement): GroupQueryBuilder[U, Query[T]] = {
    val (
      src ::
        FuncElement(c: ScalaLambdaCallee[T, U] @unchecked, _, _) ::
        ConstValueElement(provider: QueryProvider, _) ::
        _
    ) = takeParams(method, 3)

    GroupBy(of(src).asInstanceOf[QueryBuilder[T]], toLambda1(c), c.resType, method.pos)
  }

  private def ofMapValues[T, U, S](method: MethodElement): GroupQueryBuilder[T, S] = {
    val (
      src ::
        FuncElement(c: ScalaLambdaCallee[U, S] @unchecked, _, _) ::
        _
    ) = takeParams(method, 2)

    MapValues(of(src).asInstanceOf[GroupQueryBuilder[T, U]], toLambda1(c), c.resType, method.pos)
  }

  private def ofExtend[U](method: MethodElement): QueryBuilder[DynamicObject] = {
    val (
      src ::
        ConstValueElement(fieldName: String, _) ::
        FuncElement(c: ScalaLambdaCallee[DynamicObject, U] @unchecked, _, _) ::
        _
    ) = takeParams(method, 3)

    Extend(of(src), fieldName, toLambda1(c), c.resType, method.pos)
  }

  private def ofExtendTyped[T, U](method: MethodElement): QueryBuilder[_] = {
    val (src :: lambdaOrExtensions) = method.methodArgs

    lambdaOrExtensions match {
      case (lam :: Nil) if lam.name == "f" =>
        lam.param match {
          case FuncElement(c: ScalaLambdaCallee[T, U] @unchecked, _, _) =>
            ExtendTyped(of(src.param).asInstanceOf[QueryBuilder[T]], toLambda1(c), c.resType, method.pos)
        }
      case args if args.forall(_.name == "extVal") =>
        val extensions = args.map(_.param).map { case ConstValueElement(v, vt: TypeInfo[_]) => (v, vt) }
        val srcBuilder = of(src.param).asInstanceOf[QueryBuilder[Any]]
        extensions.foldLeft(srcBuilder) { case (queryBuilder, (extValue, extType)) =>
          ExtendTypedValue(queryBuilder, extValue, extType.cast[Any], method.pos).asInstanceOf[QueryBuilder[Any]]
        }
      case as => throw new MatchError(as)
    }
  }

  private def ofReplace[T, U >: T](method: MethodElement): QueryBuilder[T] = {
    val (src :: lambdaOrReplacements) = method.methodArgs

    lambdaOrReplacements match {
      case (lam :: Nil) if lam.name == "f" =>
        lam.param match {
          case FuncElement(c: ScalaLambdaCallee[T, U] @unchecked, _, _) =>
            Replace(of(src.param).asInstanceOf[QueryBuilder[T]], toLambda1(c), c.resType, method.pos)
        }
      case args if args.forall(_.name == "repVal") =>
        val replacements = args.map(_.param).map { case ConstValueElement(v, vt) => (v, vt) }
        val srcBuilder = of(src.param).asInstanceOf[QueryBuilder[T]]
        replacements.foldLeft(srcBuilder) { case (queryBuilder, (repValue, repType)) =>
          ReplaceValue(queryBuilder, repValue, repType.cast[Any], method.pos)
        }
      case as => throw new MatchError(as)
    }
  }

  private def ofUnion[T, U >: T](method: MethodElement): QueryBuilder[U] = {
    val src :: others = takeParams(method, method.methodArgs.size)
    val srcBuilder = of(src).asInstanceOf[QueryBuilder[T]]
    val otherBuilders = others.map(of(_).asInstanceOf[QueryBuilder[U]])
    Union(srcBuilder, otherBuilders, method.pos)
  }

  private def ofMerge[T, U >: T, V >: U](method: MethodElement): QueryBuilder[U] = {
    val (
      src ::
        ConstValueElement(ag: (Aggregator[V @unchecked] { type Result = V }) @unchecked, _) ::
        others
    ) = takeParams(method, method.methodArgs.size)

    val srcBuilder = of(src).asInstanceOf[QueryBuilder[T]]
    val otherBuilders = others.map(of(_).asInstanceOf[QueryBuilder[U]])
    Merge(srcBuilder, otherBuilders, ag, method.pos)
  }

  private def ofSort[T, U](method: MethodElement): QueryBuilder[T] = {
    val (
      src ::
        FuncElement(c: ScalaLambdaCallee[T, U] @unchecked, _, _) ::
        ConstValueElement(ord: Ordering[U] @unchecked, _) ::
        _
    ) = takeParams(method, 3)

    Sort(of(src).asInstanceOf[QueryBuilder[T]], toLambda1(c), ord, c.resType, method.pos)
  }

  private def ofArrange(method: MethodElement): QueryBuilder[_] = {
    val (src :: _) = takeParams(method, 1)
    Arrange(of(src), method.pos)
  }

  private def ofDifference[T, U >: T](method: MethodElement): QueryBuilder[U] = {
    val (src :: other :: _) = takeParams(method, 2)
    val srcBuilder = of(src).asInstanceOf[QueryBuilder[T]]
    val otherBuilder = of(other).asInstanceOf[QueryBuilder[U]]
    Difference(srcBuilder, otherBuilder, method.pos)
  }

  private def ofTake(method: MethodElement): QueryBuilder[_] = {
    val (
      src ::
        ConstValueElement(offset: Int, _) ::
        ConstValueElement(rowCount: Int, _) ::
        _
    ) = takeParams(method, 3)

    Take(of(src), offset, rowCount, method.pos)
  }

  private def ofDistinct(method: MethodElement): QueryBuilder[_] = {
    val (src :: _) = takeParams(method, 1)
    Distinct(of(src), method.pos)
  }

  private def ofUntype(method: MethodElement): QueryBuilder[DynamicObject] = {
    val (src :: _) = takeParams(method, 1)
    Untype(of(src), method.pos)
  }

  private def ofShape[U](method: MethodElement): QueryBuilder[U] = {
    val (
      src ::
        FuncElement(c: ScalaLambdaCallee[DynamicObject, U] @unchecked, _, _) ::
        _
    ) = takeParams(method, 2)

    ShapeTo(of(src), toLambda1(c), method.rowTypeInfo.cast[U], method.pos)
  }

  private def ofTupleJoin[L, R, B1 >: L, B2 >: R](method: MethodElement): JoinQueryBuilder[L, R] = {
    import JoinHelper._

    val args = method.methodArgs

    val onPredicate = findFuncElement(args, "on").map(_.callee)
    val onFields = findConstValueElement(args, "fields").map(_.value)

    val leftMultiKey = findFuncElement(args, "leftMultiKey").map(_.callee)
    val rightMultiKey = findFuncElement(args, "rightMultiKey").map(_.callee)

    val leftDefaultFunc = findFuncElement(args, "leftDefault")
    val rightDefaultFunc = findFuncElement(args, rightDefault)

    val joinBuilder = ofJoin[L, R](method)

    val ofOn = (onPredicate, onFields) match {
      case (_, Some(fields: Seq[String] @unchecked)) =>
        Some(JoinOnFields(_: JoinQueryBuilder[L, R], fields))
      case (Some(cPredicate: ScalaLambdaCallee2[L, R, Boolean] @unchecked), _) =>
        val fLeft = leftMultiKey.map { case cLeft: ScalaLambdaCallee[L, MultiKey] @unchecked => toLambda1(cLeft) }
        val fRight = rightMultiKey.map { case cRight: ScalaLambdaCallee[R, MultiKey] @unchecked => toLambda1(cRight) }
        Some(JoinOnPredicate(_: JoinQueryBuilder[L, R], toLambda2(cPredicate), fLeft, fRight))
      case _ =>
        None
    }

    val ofLeftDefault = leftDefaultFunc.map { case lf @ FuncElement(c: ScalaLambdaCallee[R, B1] @unchecked, _, _) =>
      JoinWithLeftDefault(_: JoinQueryBuilder[L, R], toLambda1(c), lf.rowTypeInfo.cast[B1], method.pos)
    }

    val ofRightDefault = rightDefaultFunc.map { case rf @ FuncElement(c: ScalaLambdaCallee[L, B2] @unchecked, _, _) =>
      JoinWithRightDefault(_: JoinQueryBuilder[L, R], toLambda1(c), rf.rowTypeInfo.cast[B2], method.pos)
    }

    val joinWithClausesBuilder = Seq(ofOn, ofLeftDefault, ofRightDefault).flatten.foldLeft(joinBuilder) {
      case (builder, of) => of(builder).asInstanceOf[JoinQueryBuilder[L, R]]
    }

    joinWithClausesBuilder
  }

  private def ofNaturalJoin[L, R, B1 >: L, B2 >: R](method: MethodElement): NaturalJoinQueryBuilder[L, R] = {
    import JoinHelper._

    val args = method.methodArgs

    val leftDefaultFunc = findFuncElement(args, "leftDefault")
    val rightDefaultFunc = findFuncElement(args, rightDefault)

    val joinBuilder = ofJoin[L, R](method)

    val ofLeftDefault = leftDefaultFunc.map { case lf @ FuncElement(c: ScalaLambdaCallee[R, B1] @unchecked, _, _) =>
      JoinWithLeftDefault(_: JoinQueryBuilder[L, R], toLambda1(c), lf.rowTypeInfo.cast[B1], method.pos)
    }

    val ofRightDefault = rightDefaultFunc.map { case rf @ FuncElement(c: ScalaLambdaCallee[L, B2] @unchecked, _, _) =>
      JoinWithRightDefault(_: JoinQueryBuilder[L, R], toLambda1(c), rf.rowTypeInfo.cast[B2], method.pos)
    }

    val joinWithClausesBuilder = Seq(ofLeftDefault, ofRightDefault).flatten.foldLeft(joinBuilder) {
      case (builder, of) => of(builder).asInstanceOf[JoinQueryBuilder[L, R]]
    }

    JoinOnNatural(joinWithClausesBuilder)
  }

  private def ofJoin[L, R](method: MethodElement): JoinQueryBuilder[L, R] = {
    import QueryMethod._

    val (left :: right :: _) = takeParams(method, 2)

    val leftBuilder = of(left).asInstanceOf[QueryBuilder[L]]
    val rightBuilder = of(right).asInstanceOf[QueryBuilder[R]]

    method.methodCode match {
      case INNER_JOIN | NATURAL_INNER_JOIN             => InnerJoin(leftBuilder, rightBuilder, method.pos)
      case FULL_OUTER_JOIN | NATURAL_FULL_OUTER_JOIN   => FullOuterJoin(leftBuilder, rightBuilder, method.pos)
      case LEFT_OUTER_JOIN | NATURAL_LEFT_OUTER_JOIN   => LeftOuterJoin(leftBuilder, rightBuilder, method.pos)
      case RIGHT_OUTER_JOIN | NATURAL_RIGHT_OUTER_JOIN => RightOuterJoin(leftBuilder, rightBuilder, method.pos)
    }
  }

  private def ofAggregateBy[GroupKeyType, GroupValueType, AggregateType](method: MethodElement): QueryBuilder[_] = {
    val (
      src ::
        ConstValueElement(keyType: TypeInfo[GroupKeyType] @unchecked, _) ::
        ConstValueElement(valueType: TypeInfo[GroupValueType] @unchecked, _) ::
        ConstValueElement(aggregateType: TypeInfo[AggregateType] @unchecked, _) ::
        ConstValueElement(valueKey, _) ::
        FuncElement(c: ScalaLambdaCallee[Query[GroupValueType], AggregateType] @unchecked, _, _) ::
        ConstValueElement(provider: QueryProvider, _) ::
        _
    ) = takeParams(method, 7)

    AggregateBy(of(src), toLambda1(c), keyType, valueType, aggregateType, method.pos)
  }

  private def ofAggregateByImplicit[T, GroupKeyType >: T, GroupValueType >: T, AggregateType](
      method: MethodElement
  ): QueryBuilder[_] = {
    val (
      src ::
        ConstValueElement(keyType: TypeInfo[GroupKeyType] @unchecked, _) ::
        ConstValueElement(valueType: TypeInfo[GroupValueType] @unchecked, _) ::
        ConstValueElement(aggregateType: TypeInfo[AggregateType] @unchecked, _) ::
        ConstValueElement(valueKey, _) ::
        ConstValueElement(provider: QueryProvider, _) ::
        ConstValueElement(aggregators: List[(NodeFunction1[Any, Any], TypeInfo[Any])] @unchecked, _) ::
        _
    ) = takeParams(method, 7)

    val (aggregatorNodeFuncs, aggregateTypes) = aggregators.unzip

    AggregateByImplicit(
      of(src).asInstanceOf[QueryBuilder[T]],
      aggregatorNodeFuncs,
      aggregateTypes,
      keyType,
      valueType,
      aggregateType,
      method.pos
    )
  }

  private def ofAggregateByUntyped(method: MethodElement): QueryBuilder[DynamicObject] = {
    val (
      src ::
        ConstValueElement(groupByProperties: Set[String @unchecked], _) ::
        ConstValueElement(untypedAggregations: Seq[UntypedAggregation @unchecked], _) ::
        _
    ) = takeParams(method, 3)

    AggregateByUntyped(of(src), groupByProperties, untypedAggregations, method.pos)
  }

  private def ofPermitTableScan(method: MethodElement): QueryBuilder[_] = {
    val (src :: _) = takeParams(method, 1)
    PermitTableScan(of(src), method.pos)
  }

  private def takeParams(method: MethodElement, n: Int): Seq[RelationElement] = {
    method.methodArgs.take(n).map(_.param)
  }

  private def toLambda1[A, R](c: ScalaLambdaCallee[A, R]): Lambda1[A, R] = {
    val a1 = c.lambda.right.toOption
    val a2 = c.lambda.left.toOption
    val a3 = c.lambdaFunc
    Lambda1(a1, a2, a3)
  }

  private def toLambda2[A, B, R](c: ScalaLambdaCallee2[A, B, R]): Lambda2[A, B, R] = {
    val a1 = c.lambda.right.toOption
    val a2 = c.lambda.left.toOption
    val a3 = c.lambdaFunc
    Lambda2(a1, a2, a3)
  }
}
