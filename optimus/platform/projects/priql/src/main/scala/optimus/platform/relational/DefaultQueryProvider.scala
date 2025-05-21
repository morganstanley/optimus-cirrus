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
import optimus.platform.relational.DefaultQueryProvider._
import optimus.platform.relational.aggregation._
import optimus.platform.relational.inmemory.ExtensionOptimizer
import optimus.platform.relational.inmemory.IterableRewriter
import optimus.platform.relational.inmemory.MethodKeyOptimizer
import optimus.platform.relational.inmemory.MethodPositionFinder
import optimus.platform.relational.inmemory.ShapeToRewriter
import optimus.platform.relational.internal.OptimusCoreAPI._
import optimus.platform.relational.tree.MethodArgConstants._
import optimus.platform.relational.tree._

trait DefaultQueryProvider extends QueryProvider { this: KeyPropagationPolicy =>

  def entitledOnly[T](src: Query[T])(implicit pos: MethodPosition): Query[T] = {
    src.element match {
      case eo: EntitledOnlySupport =>
        QueryImpl[T](eo.copyAsEntitledOnly(), this)
      case _ =>
        throw new RelationalUnsupportedException(entitledOnlyUnsupported)
    }
  }

  def permitTableScan[T](src: Query[T])(implicit pos: MethodPosition): Query[T] = {
    val e = src.element
    val method = new MethodElement(QueryMethod.PERMIT_TABLE_SCAN, List(MethodArg(source, e)), e.rowTypeInfo, e.key, pos)
    QueryImpl[T](method, this)
  }

  def map[T, U: TypeInfo](src: Query[T], f: Lambda1[T, U])(implicit pos: MethodPosition): Query[U] = {
    val sourceType = src.elementType
    val resultType = typeInfo[U]
    val argument = new Argument(sourceType)

    val lambda = lambdaToFuncElement(f, resultType, argument)
    val key = mkProject(src.element.key.asInstanceOf[RelationKey[T]])(sourceType.cast[T], resultType)
    val method = new MethodElement(
      QueryMethod.OUTPUT,
      List(MethodArg(source, src.element), MethodArg(function, lambda)),
      resultType,
      key,
      pos)
    QueryImpl[U](method, this)
  }

  def flatMap[T, U, F[_]](src: Query[T], f: Lambda1[T, F[U]])(implicit
      conv: QueryConverter[U, F],
      resultType: TypeInfo[U],
      pos: MethodPosition): Query[U] = {
    val sourceType = src.elementType
    val targetType = typeInfo[Query[U]]
    val argument = new Argument(sourceType)

    val lambda = f match {
      case Lambda1(Some(f), _, lambda) =>
        // we have to compose conv.convert here since
        // 1. it is a bit hard to call it when we lose the generic type information
        // 2. the provider may change when the execution occurs
        val convFunc = (t: T) => conv.convert[U](f(t), null, this)
        new FuncElement(new ScalaLambdaCallee(Right(convFunc), lambda, targetType, argument), Nil, null)
      case Lambda1(_, Some(nf), lambda) =>
        val fn = asNode.apply$withNode(nf)
        val convNodeFunc = liftNode { (t: T) =>
          conv.convert[U](fn(t), null, this)
        }
        new FuncElement(new ScalaLambdaCallee(Left(convNodeFunc), lambda, targetType, argument), Nil, null)
      case Lambda1(None, None, _) =>
        throw new IllegalArgumentException("expected either a Function or a NodeFunction but both were None")
    }
    val key = mkProject(src.element.key.asInstanceOf[RelationKey[T]])(sourceType.cast[T], resultType)
    val method = new MethodElement(
      QueryMethod.FLATMAP,
      List(MethodArg(source, src.element), MethodArg(function, lambda), MethodArg("conv", new ConstValueElement(conv))),
      resultType,
      key,
      pos)
    QueryImpl[U](method, this)
  }

  def filter[T](src: Query[T], p: Lambda1[T, Boolean])(implicit pos: MethodPosition): Query[T] = {
    val sourceType = src.elementType
    val resultType = TypeInfo.BOOLEAN
    val argument = new Argument(sourceType)

    val lambda = lambdaToFuncElement(p, resultType, argument)
    val key = src.element.key
    val method = new MethodElement(
      QueryMethod.WHERE,
      List(MethodArg(source, src.element), MethodArg(predicate, lambda)),
      sourceType,
      key,
      pos)
    QueryImpl[T](method, this)
  }

  def groupBy[T, U: TypeInfo](src: Query[T], f: Lambda1[T, U])(implicit
      pos: MethodPosition): GroupQuery[U, Query[T]] = {

    implicit val sourceType = src.elementType.cast[T]
    val uType = typeInfo[U]
    val argument = new Argument(sourceType)

    val lambda = lambdaToFuncElement(f, uType, argument)
    val key = mkPartialTuple[U, Query[T]](Left(SuperKey[U]))
    // pvd is used for sub-Query
    val method = new MethodElement(
      QueryMethod.GROUP_BY,
      List(MethodArg(source, src.element), MethodArg(function, lambda), MethodArg("pvd", new ConstValueElement(this))),
      typeInfo[(U, Query[T])],
      key,
      pos
    )
    GroupQueryImpl[U, Query[T]](method, this)
  }

  def groupBy[T, GroupKeyType: TypeInfo, GroupValueType: TypeInfo](src: Query[T])(implicit
      pos: MethodPosition): GroupQuery[GroupKeyType, Query[GroupValueType]] = {
    val keyType = typeInfo[GroupKeyType]
    val resultType = typeInfo[(GroupKeyType, Query[GroupValueType])]
    val key = mkPartialTuple[GroupKeyType, Query[GroupValueType]](Left(SuperKey[GroupKeyType]))
    val valueKey = NoKey
    val method = new MethodElement(
      QueryMethod.GROUP_BY_TYPED,
      List(
        MethodArg(source, src.element),
        MethodArg("keyType", new ConstValueElement(keyType)),
        MethodArg("valueType", new ConstValueElement(typeInfo[GroupValueType])),
        MethodArg("valueKey", new ConstValueElement(valueKey)),
        MethodArg("pvd", new ConstValueElement(this))
      ),
      resultType,
      key,
      pos
    )
    GroupQueryImpl[GroupKeyType, Query[GroupValueType]](method, this)
  }

  def aggregateBy[T, GroupKeyType: TypeInfo, GroupValueType: TypeInfo, AggregateType: TypeInfo](
      src: Query[T],
      f: Lambda1[Query[GroupValueType], AggregateType])(implicit
      pos: MethodPosition): Query[GroupKeyType with AggregateType] = {
    val keyType = typeInfo[GroupKeyType]
    val groupKey = SuperKey[GroupKeyType]
    val aggregateType = typeInfo[AggregateType]
    val resultType = TypeInfo.intersect(typeInfo[GroupKeyType], typeInfo[AggregateType])
    val valueKey = NoKey
    val argument = new Argument(typeInfo[Query[GroupValueType]])
    val lambda = lambdaToFuncElement(f, aggregateType, argument)
    val method = new MethodElement(
      QueryMethod.AGGREGATE_BY,
      List(
        MethodArg(source, src.element),
        MethodArg("groupKeyType", new ConstValueElement(typeInfo[GroupKeyType])),
        MethodArg("groupValueType", new ConstValueElement(typeInfo[GroupValueType])),
        MethodArg("aggregateType", new ConstValueElement(aggregateType)),
        MethodArg("valueKey", new ConstValueElement(valueKey)),
        MethodArg(function, lambda),
        MethodArg("pvd", new ConstValueElement(this))
      ),
      resultType,
      groupKey,
      pos
    )
    QueryImpl[GroupKeyType with AggregateType](method, this)
  }

  def aggregateBy[T, GroupKeyType: TypeInfo, GroupValueType: TypeInfo, AggregateType: TypeInfo](
      src: Query[T],
      aggregators: List[(NodeFunction1[_, _], TypeInfo[_])])(implicit
      pos: MethodPosition): Query[GroupKeyType with AggregateType] = {
    val resultType = TypeInfo.intersect(typeInfo[GroupKeyType], typeInfo[AggregateType])
    val groupKey = SuperKey[GroupKeyType]
    val valueKey = NoKey

    val combinedConstElement = MethodArg[RelationElement]("aggregators", new ConstValueElement(aggregators))
    val methodArgs = List[MethodArg[RelationElement]](
      MethodArg[RelationElement](source, src.element),
      MethodArg("groupKeyType", new ConstValueElement(typeInfo[GroupKeyType])),
      MethodArg[RelationElement]("groupValueType", new ConstValueElement(typeInfo[GroupValueType])),
      MethodArg("aggregateType", new ConstValueElement(typeInfo[AggregateType])),
      MethodArg("valueKey", new ConstValueElement(valueKey)),
      MethodArg("pvd", new ConstValueElement(this)),
      combinedConstElement
    )
    val method = new MethodElement(QueryMethod.AGGREGATE_BY_IMPLICIT, methodArgs, resultType, groupKey, pos)
    QueryImpl[GroupKeyType with AggregateType](method, this)
  }

  def aggregateByUntyped(
      src: Query[DynamicObject],
      groupByProperties: Set[String],
      untypedAggregations: UntypedAggregation*)(implicit pos: MethodPosition): Query[DynamicObject] = {
    val key = mkDynamic(groupByProperties.toSeq, DynamicObject.typeTag)
    val method = new MethodElement(
      QueryMethod.AGGREGATE_BY_UNTYPED,
      List(
        MethodArg[RelationElement](source, src.element),
        MethodArg("groupByProperties", new ConstValueElement(groupByProperties)),
        MethodArg("untypedAggregations", new ConstValueElement(untypedAggregations))
      ),
      src.elementType,
      key,
      pos
    )
    QueryImpl[DynamicObject](method, this)
  }

  def mapValues[T, U, S: TypeInfo](src: GroupQuery[T, U], f: Lambda1[U, S])(implicit
      pos: MethodPosition): GroupQuery[T, S] = {
    val sourceType = src.elementType
    implicit val tType = sourceType.typeParams(0).cast[T]
    val resultType = typeInfo[S]
    val argument = new Argument(sourceType.typeParams(1).cast[U])
    val lambda = lambdaToFuncElement(f, resultType, argument)
    val key = src.element.key
    val method = new MethodElement(
      QueryMethod.GROUP_MAP_VALUES,
      List(MethodArg(source, src.element), MethodArg(function, lambda)),
      typeInfo[(T, S)],
      key,
      pos)
    GroupQueryImpl[T, S](method, this)
  }

  def innerJoin[L, R](left: Query[L], right: Query[R])(implicit pos: MethodPosition): JoinQuery[L, R] =
    join(left, right, QueryMethod.INNER_JOIN)
  def leftOuterJoin[L, R](left: Query[L], right: Query[R])(implicit pos: MethodPosition): JoinQuery[L, R] =
    join(left, right, QueryMethod.LEFT_OUTER_JOIN)
  def rightOuterJoin[L, R](left: Query[L], right: Query[R])(implicit pos: MethodPosition): JoinQuery[L, R] =
    join(left, right, QueryMethod.RIGHT_OUTER_JOIN)
  def fullOuterJoin[L, R](left: Query[L], right: Query[R])(implicit pos: MethodPosition): JoinQuery[L, R] =
    join(left, right, QueryMethod.FULL_OUTER_JOIN)

  private def join[L, R](left: Query[L], right: Query[R], queryMethod: QueryMethod)(implicit
      pos: MethodPosition): JoinQuery[L, R] = {
    implicit val leftType = left.elementType.cast[L]
    implicit val rightType = right.elementType.cast[R]
    val key = mkTuple(left.element.key.asInstanceOf[RelationKey[L]], right.element.key.asInstanceOf[RelationKey[R]])
    val resultType = typeInfo[(L, R)]
    val method = new MethodElement(
      queryMethod,
      List(MethodArg(MethodArgConstants.left, left.element), MethodArg(MethodArgConstants.right, right.element)),
      resultType,
      key,
      pos)
    JoinQueryImpl[L, R](method, this)
  }

  def on[L, R](
      src: JoinQuery[L, R],
      f: Lambda2[L, R, Boolean],
      fLeft: Option[Lambda1[L, MultiKey]] = None,
      fRight: Option[Lambda1[R, MultiKey]] = None): JoinQuery[L, R] = {

    val MethodElement(methodCode, methodArgs, srcType, srcKey, pos) = src.element
    if (methodArgs.exists(_.name == MethodArgConstants.on))
      throw new RelationalException("'on' was already called, it could only be called once")
    val argument = new Argument(srcType)
    val lArgument = new Argument(methodArgs(0).param.rowTypeInfo)
    val rArgument = new Argument(methodArgs(1).param.rowTypeInfo)

    val (onLambda, leftMultiKeyLambda, rightMultiKeyLambda) = (f, fLeft, fRight) match {
      case (Lambda2(Some(onLambda), None, lambdaFunc), None, None) => {
        val funcElement =
          new FuncElement(new ScalaLambdaCallee2(Right(onLambda), lambdaFunc, TypeInfo.BOOLEAN, argument), Nil, null)
        (funcElement, null, null)
      }
      case (Lambda2(None, Some(onLambdaNode), lambdaFunc), None, None) => {
        val funcElement =
          new FuncElement(new ScalaLambdaCallee2(Left(onLambdaNode), lambdaFunc, TypeInfo.BOOLEAN, argument), Nil, null)
        (funcElement, null, null)
      }
      case (Lambda2(None, None, lambdaFunc), Some(leftLambda), Some(rightLambda)) => {
        val funcElement =
          new FuncElement(new ScalaLambdaCallee2(null, lambdaFunc, TypeInfo.BOOLEAN, argument), Nil, null)
        val (leftFuncElement, rightFuncElement) = ((leftLambda, rightLambda): @unchecked) match {
          case (Lambda1(Some(lLambda), None, lLambdaFunc), Lambda1(Some(rLambda), None, rLambdaFunc)) =>
            (
              new FuncElement(
                new ScalaLambdaCallee(Right(lLambda), lLambdaFunc, typeInfo[MultiKey], lArgument),
                null,
                null),
              new FuncElement(
                new ScalaLambdaCallee(Right(rLambda), rLambdaFunc, typeInfo[MultiKey], rArgument),
                null,
                null))
          case (Lambda1(Some(lLambda), None, lLambdaFunc), Lambda1(None, Some(rLambda), rLambdaFunc)) =>
            (
              new FuncElement(
                new ScalaLambdaCallee(Right(lLambda), lLambdaFunc, typeInfo[MultiKey], lArgument),
                null,
                null),
              new FuncElement(
                new ScalaLambdaCallee(Left(rLambda), rLambdaFunc, typeInfo[MultiKey], rArgument),
                null,
                null))
          case (Lambda1(None, Some(lLambda), lLambdaFunc), Lambda1(Some(rLambda), None, rLambdaFunc)) =>
            (
              new FuncElement(
                new ScalaLambdaCallee(Left(lLambda), lLambdaFunc, typeInfo[MultiKey], lArgument),
                null,
                null),
              new FuncElement(
                new ScalaLambdaCallee(Right(rLambda), rLambdaFunc, typeInfo[MultiKey], rArgument),
                null,
                null))
          case (Lambda1(None, Some(lLambda), lLambdaFunc), Lambda1(None, Some(rLambda), rLambdaFunc)) =>
            (
              new FuncElement(
                new ScalaLambdaCallee(Left(lLambda), lLambdaFunc, typeInfo[MultiKey], lArgument),
                null,
                null),
              new FuncElement(
                new ScalaLambdaCallee(Left(rLambda), rLambdaFunc, typeInfo[MultiKey], rArgument),
                null,
                null))
        }
        (funcElement, leftFuncElement, rightFuncElement)
      }
      case _ =>
        throw new RelationalException(
          s"lambda(L, R => Boolean): $f and left/right lambda (L/R => MultiKey) cannot be null at the same time")
    }
    val newMethodArgs: List[MethodArg[RelationElement]] = methodArgs ::: List(
      MethodArg[RelationElement](MethodArgConstants.on, onLambda),
      MethodArg[RelationElement]("leftMultiKey", leftMultiKeyLambda),
      MethodArg[RelationElement]("rightMultiKey", rightMultiKeyLambda)
    )
    val method = new MethodElement(methodCode, newMethodArgs, srcType, srcKey, pos)
    JoinQueryImpl[L, R](method, this)
  }

  def on[L, R](src: JoinQuery[L, R], fields: Seq[String]): JoinQuery[L, R] = {
    val MethodElement(methodCode, methodArgs, srcType, srcKey, pos) = src.element
    if (methodArgs.exists(_.name == MethodArgConstants.on))
      throw new RelationalException("'on' was already called, it could only be called once")
    val lType = methodArgs(0).param.rowTypeInfo
    val lKey = DynamicKey(fields, lType)
    val lFunc =
      if (lKey.isSyncSafe)
        Right((x: L) => new MultiKey(lKey.ofSync(x).toArray[Any]))
      else
        Left(liftNode { (x: L) =>
          new MultiKey(lKey.of(x).toArray[Any])
        })
    val lFuncElem: RelationElement =
      new FuncElement(new ScalaLambdaCallee(lFunc, None, typeInfo[MultiKey], new Argument(lType)), Nil, null)

    val rType = methodArgs(1).param.rowTypeInfo
    val rKey = DynamicKey(fields, rType)
    val rFunc =
      if (rKey.isSyncSafe)
        Right((x: R) => new MultiKey(rKey.ofSync(x).toArray[Any]))
      else
        Left(liftNode { (x: R) =>
          new MultiKey(rKey.of(x).toArray[Any])
        })
    val rFuncElem: RelationElement =
      new FuncElement(new ScalaLambdaCallee(rFunc, None, typeInfo[MultiKey], new Argument(rType)), Nil, null)

    val onElem: RelationElement =
      new FuncElement(new ScalaLambdaCallee2(null, None, TypeInfo.BOOLEAN, new Argument(srcType)), Nil, null)
    val fieldsElem: RelationElement = new ConstValueElement(fields, typeInfo[Seq[String]])

    val newMethodArgs: List[MethodArg[RelationElement]] = methodArgs ::: List(
      MethodArg(MethodArgConstants.on, onElem),
      MethodArg("leftMultiKey", lFuncElem),
      MethodArg("rightMultiKey", rFuncElem),
      MethodArg("fields", fieldsElem))
    val method = new MethodElement(methodCode, newMethodArgs, srcType, srcKey, pos)
    JoinQueryImpl[L, R](method, this)
  }

  def onNatural[L, R](src: JoinQuery[L, R])(implicit pos: MethodPosition): NaturalJoinQuery[L, R] = {
    import QueryMethod._

    val MethodElement(methodCode, methodArgs, _, srcKey: RelationKey[Any @unchecked], pos) = src.element
    if (methodArgs.exists(_.name == MethodArgConstants.on))
      throw new RelationalException("'on' was already called!")
    val left = methodArgs(0).param
    val right = methodArgs(1).param
    val leftType = left.rowTypeInfo.cast[L]
    val rightType = right.rowTypeInfo.cast[R]

    if (leftType.concreteClass.isDefined && rightType.concreteClass.isDefined)
      throw new RelationalException(
        "Cannot perform natural join between two concrete row types. At least one side must be a trait.")
    val leftColumns = leftType.propertyNames
    val rightColumns = rightType.propertyNames
    if (leftColumns.intersect(rightColumns).isEmpty)
      throw new RelationalException(
        s"Unable to perform natural join between $leftType and $rightType. No common columns.")

    val naturalQueryMethod = methodCode match {
      case INNER_JOIN       => NATURAL_INNER_JOIN
      case LEFT_OUTER_JOIN  => NATURAL_LEFT_OUTER_JOIN
      case RIGHT_OUTER_JOIN => NATURAL_RIGHT_OUTER_JOIN
      case FULL_OUTER_JOIN  => NATURAL_FULL_OUTER_JOIN
      case _                => throw new RelationalException(s"Invalid query method: $methodCode for onNatural")
    }

    val resultType = TypeInfo.intersect(leftType, rightType)
    val key = mkFlatten[L, R](srcKey)(leftType, rightType, resultType)
    val method = new MethodElement(naturalQueryMethod, methodArgs, resultType, key, pos)
    NaturalJoinQueryImpl[L, R](method, this)
  }

  def withLeftDefault[L, R, B >: L](src: JoinQuery[L, R], f: Lambda1[R, B])(implicit
      bType: TypeInfo[B],
      pos: MethodPosition): JoinQuery[B, R] = {
    import QueryMethod._

    val MethodElement(methodCode, methodArgs, _, srcKey, pos) = src.element
    if (methodCode == INNER_JOIN || methodCode == LEFT_OUTER_JOIN)
      throw new RelationalException("withLeftDefault could only be called on full-outer-join or right-outer-join")
    if (methodArgs.exists(_.name == "leftDefault"))
      throw new RelationalException("withLeftDefault already be called, it could only be called once")

    val left = methodArgs(0).param.asInstanceOf[MultiRelationElement]
    val right = methodArgs(1).param.asInstanceOf[MultiRelationElement]
    val leftType = left.rowTypeInfo.cast[L]
    implicit val rightType = right.rowTypeInfo.cast[R]
    val resultType = typeInfo[(B, R)]

    val key =
      if (srcKey == NoKey || bType =:= leftType) srcKey
      else {
        val upcastKey = mkUpcast[L, B](left.key.asInstanceOf[RelationKey[L]])(leftType, bType)
        mkTuple[B, R](upcastKey, right.key.asInstanceOf[RelationKey[R]])(bType, rightType)
      }

    val func = lambdaToFuncElement(f, bType, new Argument(rightType))
    val method = new MethodElement(
      methodCode,
      methodArgs.:+(MethodArg[RelationElement]("leftDefault", func)),
      resultType,
      key,
      pos)
    JoinQueryImpl[B, R](method, this)
  }

  def withRightDefault[L, R, B >: R](src: JoinQuery[L, R], f: Lambda1[L, B])(implicit
      bType: TypeInfo[B],
      pos: MethodPosition): JoinQuery[L, B] = {
    import QueryMethod._

    val MethodElement(methodCode, methodArgs, _, srcKey, pos) = src.element
    if (methodCode == INNER_JOIN || methodCode == RIGHT_OUTER_JOIN)
      throw new RelationalException("withRightDefault could only be called on full-outer-join or left-outer-join")
    if (methodArgs.exists(_.name == rightDefault))
      throw new RelationalException("withRightDefault already be called, it could only be called once")

    val left = methodArgs(0).param.asInstanceOf[MultiRelationElement]
    val right = methodArgs(1).param.asInstanceOf[MultiRelationElement]
    implicit val leftType = left.rowTypeInfo.cast[L]
    val rightType = right.rowTypeInfo.cast[R]
    val resultType = typeInfo[(L, B)]

    val key =
      if (srcKey == NoKey || bType =:= rightType) srcKey
      else {
        val upcastKey = mkUpcast[R, B](right.key.asInstanceOf[RelationKey[R]])(rightType, bType)
        mkTuple[L, B](left.key.asInstanceOf[RelationKey[L]], upcastKey)(leftType, bType)
      }

    val func = lambdaToFuncElement(f, bType, new Argument(leftType))

    val method =
      new MethodElement(methodCode, methodArgs.:+(MethodArg[RelationElement](rightDefault, func)), resultType, key, pos)
    JoinQueryImpl[L, B](method, this)
  }

  def withLeftDefaultNatural[L, R, B >: L](src: NaturalJoinQuery[L, R], f: Lambda1[R, B])(implicit
      bType: TypeInfo[B],
      pos: MethodPosition): NaturalJoinQuery[B, R] = {
    import QueryMethod._

    val MethodElement(methodCode, methodArgs, _, srcKey, pos) = src.element
    if (methodCode == NATURAL_INNER_JOIN || methodCode == NATURAL_LEFT_OUTER_JOIN)
      throw new RelationalException(
        "withLeftDefault could only be called on natural-full-outer-join or natural-right-outer-join")
    if (methodArgs.exists(_.name == "leftDefault"))
      throw new RelationalException("withLeftDefault already be called, it could only be called once")

    val left = methodArgs(0).param.asInstanceOf[MultiRelationElement]
    val right = methodArgs(1).param.asInstanceOf[MultiRelationElement]
    val leftType = left.rowTypeInfo.cast[L]
    val rightType = right.rowTypeInfo.cast[R]
    val resultType = TypeInfo.intersect(bType, rightType).cast[B with R]

    val key =
      if (srcKey == NoKey || bType =:= leftType) srcKey
      else {
        val upcastKey = mkUpcast[L, B](left.key.asInstanceOf[RelationKey[L]])(leftType, bType)
        val tupleKey = mkTuple[B, R](upcastKey, right.key.asInstanceOf[RelationKey[R]])(bType, rightType)
        mkFlatten[B, R](tupleKey)(bType, rightType, resultType)
      }

    val func = lambdaToFuncElement(f, bType, new Argument(rightType))
    val method = new MethodElement(
      methodCode,
      methodArgs.:+(MethodArg[RelationElement]("leftDefault", func)),
      resultType,
      key,
      pos)
    NaturalJoinQueryImpl[B, R](method, this)
  }

  def withRightDefaultNatural[L, R, B >: R](src: NaturalJoinQuery[L, R], f: Lambda1[L, B])(implicit
      bType: TypeInfo[B],
      pos: MethodPosition): NaturalJoinQuery[L, B] = {
    import QueryMethod._

    val MethodElement(methodCode, methodArgs, _, srcKey, pos) = src.element
    if (methodCode == NATURAL_INNER_JOIN || methodCode == NATURAL_RIGHT_OUTER_JOIN)
      throw new RelationalException(
        "withRightDefault could only be called on natural-full-outer-join or natural-left-outer-join")
    if (methodArgs.exists(_.name == rightDefault))
      throw new RelationalException("withRightDefault already be called, it could only be called once")

    val left = methodArgs(0).param.asInstanceOf[MultiRelationElement]
    val right = methodArgs(1).param.asInstanceOf[MultiRelationElement]
    val leftType = left.rowTypeInfo.cast[L]
    val rightType = right.rowTypeInfo.cast[R]
    val resultType = TypeInfo.intersect(leftType, bType).cast[L with B]

    val key =
      if (srcKey == NoKey || bType =:= rightType) srcKey
      else {
        val upcastKey = mkUpcast[R, B](right.key.asInstanceOf[RelationKey[R]])(rightType, bType)
        val tupleKey = mkTuple[L, B](left.key.asInstanceOf[RelationKey[L]], upcastKey)(leftType, bType)
        mkFlatten[L, B](tupleKey)(leftType, bType, resultType)
      }

    val func = lambdaToFuncElement(f, bType, new Argument(leftType))
    val method =
      new MethodElement(methodCode, methodArgs.:+(MethodArg[RelationElement](rightDefault, func)), resultType, key, pos)
    NaturalJoinQueryImpl[L, B](method, this)
  }

  def union[T](src: Query[T], others: Query[T]*)(implicit pos: MethodPosition): Query[T] = {
    val (resultType, key) =
      mkUnion(src.elementType +: others.map(_.elementType), src.element.key +: others.map(_.element.key))
    val newArgs = src.element match {
      case MethodElement(QueryMethod.UNION, srcArg :: otherArgs, _, _, _) =>
        srcArg :: others.foldLeft(otherArgs)((args, q) => MethodArg[RelationElement]("others", q.element) :: args)
      case _ =>
        {
          MethodArg[RelationElement](source, src.element) +: others.map(q =>
            MethodArg[RelationElement]("others", q.element))
        }.toList
    }
    val method = new MethodElement(QueryMethod.UNION, newArgs, resultType, key, pos)
    QueryImpl[T](method, this)
  }

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
      pos: MethodPosition): Query[T] = {
    val (resultType, unionKey) =
      mkUnion(src.elementType +: others.map(_.elementType), src.element.key +: others.map(_.element.key))
    val newArgs = src.element match {
      case MethodElement(QueryMethod.MERGE, srcArg :: ag :: otherArgs, _, _, _) =>
        srcArg :: ag :: others.foldLeft(otherArgs)((args, q) => MethodArg[RelationElement]("others", q.element) :: args)
      case _ =>
        MethodArg[RelationElement](source, src.element) :: MethodArg[RelationElement](
          "ag",
          ElementFactory.constant(ag)) :: others
          .map(q => MethodArg[RelationElement]("others", q.element))
          .toList
    }
    val groupKey = ModuloKey(resultType, ag.resultType.cast[Any])
    val key = if (groupKey.fields.toSet.subsetOf(unionKey.fields.toSet)) unionKey else groupKey
    val method = new MethodElement(QueryMethod.MERGE, newArgs, resultType, key, pos)
    QueryImpl[T](method, this)
  }

  def difference[T](src: Query[T], other: Query[T])(implicit pos: MethodPosition): Query[T] = {
    val (resultType, key) = mkUnion(Seq(src.elementType, other.elementType), Seq(src.element.key, other.element.key))
    val method = new MethodElement(
      QueryMethod.DIFFERENCE,
      List(MethodArg(source, src.element), MethodArg("other", other.element)),
      resultType,
      key,
      pos)
    QueryImpl[T](method, this)
  }

  def untype[T](src: Query[T])(implicit pos: MethodPosition): Query[DynamicObject] = {
    val sourceType = src.elementType
    val resultType = DynamicObject.typeTag

    val key = mkDynamic(src.element.key.asInstanceOf[RelationKey[T]], null)(sourceType.cast[T])
    val method = new MethodElement(QueryMethod.UNTYPE, List(MethodArg(source, src.element)), resultType, key, pos)
    QueryImpl[DynamicObject](method, this)
  }

  def extend[U](src: Query[DynamicObject], field: String, f: Lambda1[DynamicObject, U])(implicit
      fieldType: TypeInfo[U],
      pos: MethodPosition): Query[DynamicObject] = {
    val sourceType = src.elementType
    val argument = new Argument(sourceType)

    val lambda = lambdaToFuncElement(f, fieldType, argument)
    val key = mkDynamic(src.element.key.asInstanceOf[RelationKey[DynamicObject]], field)
    val method = new MethodElement(
      QueryMethod.EXTEND,
      List(
        MethodArg(source, src.element),
        MethodArg("field", new ConstValueElement(field)),
        MethodArg(function, lambda)),
      DynamicObject.typeTag,
      key,
      pos
    )
    QueryImpl[DynamicObject](method, this)
  }

  def shapeTo[U](src: Query[DynamicObject], f: Lambda1[DynamicObject, U])(implicit
      shapeToType: TypeInfo[U],
      pos: MethodPosition): Query[U] = {
    val sourceType = src.elementType
    val argument = new Argument(sourceType)
    val lambda = lambdaToFuncElement(f, shapeToType, argument)

    val key = mkKey[U]
    val method = new MethodElement(
      QueryMethod.SHAPE,
      List(MethodArg(source, src.element), MethodArg(function, lambda)),
      shapeToType,
      key,
      pos)
    QueryImpl[U](method, this)
  }

  def extendTyped[T, U: TypeInfo](src: Query[T], f: Lambda1[T, U])(implicit pos: MethodPosition): Query[T with U] = {
    val sourceType = src.elementType.cast[T]
    val extensionType = typeInfo[U]
    val resultType = TypeInfo.intersect(sourceType, extensionType)
    val argument = new Argument(sourceType)

    val lambda = lambdaToFuncElement(f, extensionType, argument)
    val key = mkComposite[T, U](src.element.key.asInstanceOf[RelationKey[T]])(sourceType, extensionType, resultType)
    val method = new MethodElement(
      QueryMethod.EXTEND_TYPED,
      List(MethodArg(source, src.element), MethodArg(function, lambda)),
      resultType,
      key,
      pos)
    QueryImpl[T with U](method, this)
  }

  def extendTypedValue[T, U: TypeInfo](src: Query[T], extVal: U)(implicit pos: MethodPosition): Query[T with U] = {
    val sourceType = src.elementType.cast[T]
    val extensionType = typeInfo[U]
    val resultType = TypeInfo.intersect(sourceType, extensionType)

    val key = mkComposite[T, U](src.element.key.asInstanceOf[RelationKey[T]])(sourceType, extensionType, resultType)
    val valueElement = new ConstValueElement(extVal, extensionType)
    val method = new MethodElement(
      QueryMethod.EXTEND_TYPED,
      List(MethodArg(source, src.element), MethodArg("extVal", valueElement)),
      resultType,
      key,
      pos)
    QueryImpl[T with U](method, this)
  }

  def replace[T, U >: T: TypeInfo](src: Query[T], f: Lambda1[T, U])(implicit pos: MethodPosition): Query[T] = {
    val sourceType = src.elementType.cast[T]
    val replaceType = typeInfo[U]
    val argument = new Argument(sourceType)

    val lambda = lambdaToFuncElement(f, replaceType, argument)
    val key = src.element.key
    val method = new MethodElement(
      QueryMethod.REPLACE,
      List(MethodArg(source, src.element), MethodArg(function, lambda)),
      sourceType,
      key,
      pos)
    QueryImpl[T](method, this)
  }

  def replaceValue[T, U >: T: TypeInfo](src: Query[T], repVal: U)(implicit pos: MethodPosition): Query[T] = {
    val sourceType = src.elementType.cast[T]
    val replaceType = typeInfo[U]

    val key = src.element.key
    val valueElement = new ConstValueElement(repVal, replaceType)
    val method = new MethodElement(
      QueryMethod.REPLACE,
      List(MethodArg(source, src.element), MethodArg("repVal", valueElement)),
      sourceType,
      key,
      pos)
    QueryImpl[T](method, this)
  }

  def extendTypedOrReplaceValue[T, U: TypeInfo](src: Query[T], value: U)(implicit
      pos: MethodPosition): Query[T with U] = {
    val sourceType = src.elementType.cast[T]
    val replaceType = typeInfo[U]
    val resultType = TypeInfo.intersect(sourceType, replaceType)
    val key = mkComposite[T, U](src.element.key.asInstanceOf[RelationKey[T]])(sourceType, replaceType, resultType)
    val valueElement = new ConstValueElement(value, replaceType)
    val method = new MethodElement(
      QueryMethod.REPLACE,
      List(MethodArg(source, src.element), MethodArg("repVal", valueElement)),
      resultType,
      key,
      pos)
    QueryImpl[T with U](method, this)
  }

  def sortBy[T, U](src: Query[T], f: Lambda1[T, U])(implicit
      ordering: Ordering[U],
      sortType: TypeInfo[U],
      pos: MethodPosition): Query[T] = {

    val sourceType = src.elementType
    val argument = new Argument(sourceType)

    val lambda = lambdaToFuncElement(f, sortType, argument)
    val key = src.element.key
    val method = new MethodElement(
      QueryMethod.SORT,
      List(
        MethodArg(source, src.element),
        MethodArg(function, lambda),
        MethodArg("ordering", new ConstValueElement(ordering))),
      sourceType,
      key,
      pos
    )
    QueryImpl[T](method, this)
  }

  /*
   * arranged rule:
   *
   * 1 if the original element ProviderRelation'arrange is true it is arranged (for join, we should judge every branch
   * is arranged) otherwise
   *
   * 2 if one of the elements in element chain is arranged or take it is arranged (for join, we should judge every
   * branch is arranged) otherwise
   *
   * 3 arrange
   *
   *    3.1 check if all priql key are comparable if they are we could use distinct logic to sort on priql key hash
   *    code first and then sort on all priql key fields if they have the same hash code value
   *
   *        3.1.1 priql key contains CompiledKey (whose fields is enough to distinguish rows), EntityKey (who has @key,
   *        we could do the same as CompiledKey) EntityKey (who does not have key(which is marked @key), we may use
   *        EntityRef in future, but now go to step 3.2), DynamicKey(could not judge whether key has the corresponding
   *        Ordering on this level, need to go to step 3.2). for simplicity, we only consider the current level not
   *        traversing the whole RelationElement tree to insert 'arrange'
   *
   * 3.2 throw an exception
   */
  def arrange[T](src: Query[T])(implicit pos: MethodPosition): Query[T] = {
    if (ArrangedInspector.isArranged(src.element)) src
    else {
      if (ArrangedInspector.isKeyComparable(src.element.key)) {
        val key = src.element.key
        val sourceType = src.elementType

        val method = new MethodElement(QueryMethod.ARRANGE, List(MethodArg(source, src.element)), sourceType, key, pos)
        QueryImpl[T](method, this)
      } else
        throw new RelationalException(
          s"arrange can not be applied to query: ${src.element} since some of its priql key cannot do compare.")
    }
  }

  def distinct[T](src: Query[T])(implicit pos: MethodPosition): Query[T] = {
    val key = src.element.key
    if (key == null || key == NoKey) {
      val sourceType = src.elementType
      val method =
        new MethodElement(QueryMethod.TAKE_DISTINCT, List(MethodArg(source, src.element)), sourceType, key, pos)
      QueryImpl[T](method, this)
    } else src
  }

  def take[T](src: Query[T], offset: Int, numRows: Int)(implicit pos: MethodPosition): Query[T] = {
    if (numRows <= 0)
      throw new RelationalUnsupportedException("Only positive 'numRows' is supported")

    val sourceType = src.elementType
    val key = src.element.key
    val method = new MethodElement(
      QueryMethod.TAKE,
      List(
        MethodArg(source, src.element),
        MethodArg("offset", new ConstValueElement(offset)),
        MethodArg("numRows", new ConstValueElement(numRows))),
      sourceType,
      key,
      pos
    )
    QueryImpl[T](method, this)
  }

  @async def reduce[T, U >: T](src: Query[T], op: Lambda2[U, U, U]): U = {
    op match {
      case Lambda2(Some(f), _, _) =>
        val reducer = Aggregator.reduce(f)(src.element.rowTypeInfo.cast[U])
        execute[U](aggregateAsElement(src, reducer)).apply()
      case Lambda2(_, Some(nf), _) =>
        val seq = execute[Iterable[U]](src.element).apply()
        if (seq.isEmpty)
          throw new RelationalException("Sequence contains no elements")
        val f = asNode.apply$withNode(nf)
        seq.tail.aseq.foldLeft(seq.head)(f(_, _))
      case Lambda2(None, None, _) =>
        throw new IllegalArgumentException("expected either a Function or a NodeFunction but both were None")
    }
  }

  @async def foldLeft[T, U](src: Query[T], z: U)(op: Lambda2[U, T, U]): U = {
    op match {
      case Lambda2(Some(f), _, _) =>
        val folder = Aggregator.foldLeft[U, T](z)(f)
        execute[U](aggregateAsElement(src, folder)).apply()
      case Lambda2(_, Some(nf), _) =>
        val f = asNode.apply$withNode(nf)
        execute[Iterable[T]](src.element).apply().aseq.foldLeft(z)(f(_, _))
      case Lambda2(None, None, _) =>
        throw new IllegalArgumentException("expected either a Function or a NodeFunction but both were None")
    }
  }

  private def aggregateAsElement[T](src: Query[T], aggregator: Aggregator[T]): RelationElement = {
    implicit val tType = src.elementType.cast[T]
    val arguments = List(new ConstValueElement(aggregator, typeInfo[Aggregator[T]]))
    ElementFactory.call(
      src.element,
      new RuntimeMethodDescriptor(typeInfo[Query[T]], "aggregate", aggregator.resultType, List(tType)),
      arguments)
  }

  @async def aggregate[T](src: Query[T], aggregator: Aggregator[T]): aggregator.Result = {
    val func = aggregateAsElement(src, aggregator)
    execute[aggregator.Result](func).apply()
  }

  @async def aggregate[T, U](src: Query[T], f: Lambda1[T, U], aggregator: Aggregator[U])(implicit
      aggregateType: TypeInfo[U]): aggregator.Result = {
    implicit val tType = src.elementType.cast[T]
    val argument = new Argument(tType)
    val lambda = lambdaToFuncElement(f, aggregateType, argument)
    val arguments = List(lambda, new ConstValueElement(aggregator, typeInfo[Aggregator[U]]))
    val func = ElementFactory.call(
      src.element,
      new RuntimeMethodDescriptor(typeInfo[Query[T]], "aggregate", aggregator.resultType, List(aggregateType)),
      arguments)
    execute[aggregator.Result](func).apply()
  }

  @async def sum[T](src: Query[T])(implicit sum: Sum[T]): T = {
    implicit val tType = src.elementType.cast[T]
    val arguments = List(new ConstValueElement(sum, typeInfo[Sum[T]]))
    val func = ElementFactory.call(
      src.element,
      new RuntimeMethodDescriptor(typeInfo[Query[T]], "sum", tType, List(tType)),
      arguments)
    execute[T](func).apply()
  }

  @async def sum[T, U](src: Query[T], f: Lambda1[T, U])(implicit sum: Sum[U]): U = {
    implicit val tType = src.elementType.cast[T]
    implicit val uType = sum.resultType
    val argument = new Argument(tType)
    val lambda = lambdaToFuncElement(f, uType, argument)
    val arguments = List(lambda, new ConstValueElement(sum, typeInfo[Sum[T]]))
    val func = ElementFactory.call(
      src.element,
      new RuntimeMethodDescriptor(typeInfo[Query[T]], "sum", uType, List(uType)),
      arguments)
    execute[U](func).apply()
  }

  @async def average[T](src: Query[T])(implicit avg: Avg[T]): avg.Result = {
    implicit val tType = src.elementType.cast[T]
    val arguments = List(new ConstValueElement(avg, typeInfo[Avg[T]]))
    val func = ElementFactory.call(
      src.element,
      new RuntimeMethodDescriptor(typeInfo[Query[T]], "average", avg.resultType, List(tType)),
      arguments)
    execute[avg.Result](func).apply()
  }

  @async def average[T, U](src: Query[T], f: Lambda1[T, U])(implicit avg: Avg[U]): avg.Result = {
    implicit val tType = src.elementType.cast[T]
    implicit val uType = avg.valueType
    val argument = new Argument(tType)
    val lambda = lambdaToFuncElement(f, uType, argument)
    val arguments = List(lambda, new ConstValueElement(avg, typeInfo[Avg[U]]))
    val func = ElementFactory.call(
      src.element,
      new RuntimeMethodDescriptor(typeInfo[Query[T]], "average", avg.resultType, List(uType)),
      arguments)
    execute[avg.Result](func).apply()
  }

  @async def variance[T](src: Query[T])(implicit v: Variance[T]): v.Result = {
    implicit val tType = src.elementType.cast[T]
    val arguments = List(new ConstValueElement(v, typeInfo[Variance[T]]))
    val func = ElementFactory.call(
      src.element,
      new RuntimeMethodDescriptor(typeInfo[Query[T]], "variance", v.resultType, List(tType)),
      arguments)
    execute[v.Result](func).apply()
  }

  @async def variance[T, U](src: Query[T], f: Lambda1[T, U])(implicit v: Variance[U]): v.Result = {
    implicit val tType = src.elementType.cast[T]
    implicit val uType = v.valueType
    val argument = new Argument(tType)
    val lambda = lambdaToFuncElement(f, uType, argument)
    val arguments = List(lambda, new ConstValueElement(v, typeInfo[Variance[U]]))
    val func = ElementFactory.call(
      src.element,
      new RuntimeMethodDescriptor(typeInfo[Query[T]], "variance", v.resultType, List(uType)),
      arguments)
    execute[v.Result](func).apply()
  }

  @async def stddev[T](src: Query[T])(implicit stdDev: StandardDeviation[T]): stdDev.Result = {
    implicit val tType = src.elementType.cast[T]
    val arguments = List(new ConstValueElement(stdDev, typeInfo[StandardDeviation[T]]))
    val func = ElementFactory.call(
      src.element,
      new RuntimeMethodDescriptor(typeInfo[Query[T]], "stddev", stdDev.resultType, List(tType)),
      arguments)
    execute[stdDev.Result](func).apply()
  }

  @async def stddev[T, U](src: Query[T], f: Lambda1[T, U])(implicit stdDev: StandardDeviation[U]): stdDev.Result = {
    implicit val tType = src.elementType.cast[T]
    implicit val uType = stdDev.valueType
    val argument = new Argument(tType)
    val lambda = lambdaToFuncElement(f, uType, argument)
    val arguments = List(lambda, new ConstValueElement(stdDev, typeInfo[StandardDeviation[U]]))
    val func = ElementFactory.call(
      src.element,
      new RuntimeMethodDescriptor(typeInfo[Query[T]], "stddev", stdDev.resultType, List(uType)),
      arguments)
    execute[stdDev.Result](func).apply()
  }

  @async def corr[T](src: Query[T], x: Lambda1[T, Double], y: Lambda1[T, Double])(implicit
      cor: Correlation[Double]): Double = {
    implicit val tType = src.elementType.cast[T]
    val argument = new Argument(tType)
    val lambda1 = lambdaToFuncElement(x, TypeInfo.DOUBLE, argument)
    val lambda2 = lambdaToFuncElement(y, TypeInfo.DOUBLE, argument)
    val arguments = List(lambda1, lambda2, new ConstValueElement(cor, typeInfo[Correlation[T]]))
    val func = ElementFactory.call(
      src.element,
      new RuntimeMethodDescriptor(typeInfo[Query[T]], "corr", tType, List(tType)),
      arguments)
    execute[Double](func).apply()
  }

  @async def min[T](src: Query[T])(implicit min: Min[T]): T = {
    implicit val tType = src.elementType.cast[T]
    val arguments = List(new ConstValueElement(min, typeInfo[Min[T]]))
    val func = ElementFactory.call(
      src.element,
      new RuntimeMethodDescriptor(typeInfo[Query[T]], "min", tType, List(tType)),
      arguments)
    execute[T](func).apply()
  }

  @async def min[T, U](src: Query[T], f: Lambda1[T, U])(implicit min: Min[U]): U = {
    implicit val tType = src.elementType.cast[T]
    implicit val uType = min.resultType
    val argument = new Argument(tType)
    val lambda = lambdaToFuncElement(f, uType, argument)
    val arguments = List(lambda, new ConstValueElement(min, typeInfo[Min[T]]))
    val func = ElementFactory.call(
      src.element,
      new RuntimeMethodDescriptor(typeInfo[Query[T]], "min", uType, List(uType)),
      arguments)
    execute[U](func).apply()
  }

  @async def max[T](src: Query[T])(implicit max: Max[T]): T = {
    implicit val tType = src.elementType.cast[T]
    val arguments = List(new ConstValueElement(max, typeInfo[Max[T]]))
    val func = ElementFactory.call(
      src.element,
      new RuntimeMethodDescriptor(typeInfo[Query[T]], "max", tType, List(tType)),
      arguments)
    execute[T](func).apply()
  }

  @async def max[T, U](src: Query[T], f: Lambda1[T, U])(implicit max: Max[U]): U = {
    implicit val tType = src.elementType.cast[T]
    implicit val uType = max.resultType
    val argument = new Argument(tType)
    val lambda = lambdaToFuncElement(f, uType, argument)
    val arguments = List(lambda, new ConstValueElement(max, typeInfo[Max[T]]))
    val func = ElementFactory.call(
      src.element,
      new RuntimeMethodDescriptor(typeInfo[Query[T]], "max", uType, List(uType)),
      arguments)
    execute[U](func).apply()
  }

  @async def count[T](src: Query[T]): Long = {
    implicit val tType = src.elementType.cast[T]
    val arguments = List(new ConstValueElement(Count.Any, typeInfo[Count[T]]))
    val func = ElementFactory.call(
      src.element,
      new RuntimeMethodDescriptor(typeInfo[Query[T]], "count", TypeInfo.LONG),
      arguments)
    execute[Long](func).apply()
  }

  @async def count[T](src: Query[T], f: Lambda1[T, Boolean]): Long = {
    implicit val tType = src.elementType.cast[T]
    val argument = new Argument(tType)
    val lambda = lambdaToFuncElement(f, TypeInfo.BOOLEAN, argument)
    val arguments = List(lambda, new ConstValueElement(Count.Boolean, typeInfo[Count[Boolean]]))
    val func = ElementFactory.call(
      src.element,
      new RuntimeMethodDescriptor(typeInfo[Query[T]], "count", TypeInfo.LONG),
      arguments)
    execute[Long](func).apply()
  }

  @async def head[T](src: Query[T]): T = {
    implicit val tType = src.elementType.cast[T]
    val func = ElementFactory.call(src.element, new RuntimeMethodDescriptor(typeInfo[Query[T]], "head", tType), Nil)
    execute[T](func).apply()
  }

  @async def headOption[T](src: Query[T]): Option[T] = {
    implicit val tType = src.elementType.cast[T]
    val func = ElementFactory.call(
      src.element,
      new RuntimeMethodDescriptor(typeInfo[Query[T]], "headOption", typeInfo[Option[T]]),
      Nil)
    execute[Option[T]](func).apply()
  }

  @async def last[T](src: Query[T]): T = {
    implicit val tType = src.elementType.cast[T]
    val func = ElementFactory.call(src.element, new RuntimeMethodDescriptor(typeInfo[Query[T]], "last", tType), Nil)
    execute[T](func).apply()
  }

  @async def lastOption[T](src: Query[T]): Option[T] = {
    implicit val tType = src.elementType.cast[T]
    val func = ElementFactory.call(
      src.element,
      new RuntimeMethodDescriptor(typeInfo[Query[T]], "lastOption", typeInfo[Option[T]]),
      Nil)
    execute[Option[T]](func).apply()
  }

  def execute[T](e: RelationElement, executeOptions: ExecuteOptions = ExecuteOptions.Default): NodeFunction0[T] = {
    val optimized = DefaultQueryProvider.optimizeTree(e, executeOptions)
    IterableRewriter.rewriteAndCompile[T](optimized)
  }

  def createQuery[T](e: MultiRelationElement): Query[T] = {
    import QueryMethod._
    e match {
      case MethodElement(GROUP_BY | GROUP_BY_TYPED | GROUP_MAP_VALUES, _, _, _, _) =>
        GroupQueryImpl(e, this).asInstanceOf[Query[T]]
      case MethodElement(INNER_JOIN | LEFT_OUTER_JOIN | RIGHT_OUTER_JOIN | FULL_OUTER_JOIN, _, _, _, _) =>
        JoinQueryImpl(e, this).asInstanceOf[Query[T]]
      case MethodElement(
            NATURAL_INNER_JOIN | NATURAL_LEFT_OUTER_JOIN | NATURAL_RIGHT_OUTER_JOIN | NATURAL_FULL_OUTER_JOIN,
            _,
            _,
            _,
            _) =>
        NaturalJoinQueryImpl(e, this).asInstanceOf[Query[T]]
      case _ => QueryImpl[T](e, this)
    }
  }
}

object DefaultQueryProvider {
  val entitledOnlyUnsupported = "'.entitledOnly' is supported only in DAL query and used immediately after 'from'."

  def optimizeTree(e: RelationElement, executeOptions: ExecuteOptions = ExecuteOptions.Default): RelationElement = {
    val mp = MethodPositionFinder.find(e)
    val execOpts = executeOptions.copy(pos = mp)
    val ele = MethodKeyOptimizer.optimize(e)
    val reduced =
      ReducerLocator.locate(ele, ExecutionCategory.Execute).foldLeft(ele)((tree, r) => r.reduce(tree, execOpts))
    val optimized = ExtensionOptimizer.optimize(reduced)
    ShapeToRewriter.rewrite(optimized)
  }

  private[relational] def lambdaToFuncElement[T, U](
      f: Lambda1[T, U],
      resultType: TypeInfo[U],
      argument: Argument): FuncElement =
    f match {
      case Lambda1(Some(f), _, lambda) =>
        new FuncElement(new ScalaLambdaCallee(Right(f), lambda, resultType, argument), Nil, null)
      case Lambda1(_, Some(nf), lambda) =>
        new FuncElement(new ScalaLambdaCallee(Left(nf), lambda, resultType, argument), Nil, null)
      case Lambda1(None, None, _) =>
        throw new IllegalArgumentException("expected either a Function or a NodeFunction but both were None")
    }
}

private final case class QueryImpl[T](val element: MultiRelationElement, val provider: QueryProvider) extends Query[T]
private final case class JoinQueryImpl[T1, T2](val element: MultiRelationElement, val provider: QueryProvider)
    extends JoinQuery[T1, T2]
private final case class NaturalJoinQueryImpl[T1, T2](val element: MultiRelationElement, val provider: QueryProvider)
    extends NaturalJoinQuery[T1, T2]
private final case class GroupQueryImpl[T1, T2](val element: MultiRelationElement, val provider: QueryProvider)
    extends GroupQuery[T1, T2]
