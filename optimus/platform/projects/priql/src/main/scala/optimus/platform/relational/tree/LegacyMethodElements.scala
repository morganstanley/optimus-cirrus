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
package optimus.platform.relational.tree

import optimus.platform._
import optimus.platform._

import scala.collection.mutable

object AggregateByUntypedMethodElement {
  import optimus.platform.relational.accumulators.untyped._

  def apply(
      source: MultiRelationElement,
      groupByParameters: Set[String],
      accumulatorBuilders: Iterable[UntypedAccumulatorBuilder],
      key: RelationKey[_],
      pos: MethodPosition): MethodElement = {
    val args = new mutable.ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg("src", source)
    args += new MethodArg(
      "groupByParameters",
      new ExpressionListElement(groupByParameters.map(p => new ConstValueElement(p)).toList, TypeInfo.STRING))
    args += new MethodArg(
      "accumulatorBuilders",
      new ExpressionListElement(
        accumulatorBuilders.map(a => new ConstValueElement(a)).toList,
        TypeInfo.javaTypeInfo(classOf[(String, UntypedAccumulatorBuilder)]))
    )
    new MethodElement(QueryMethod.AGGREGATE_BY_UNTYPED, args.toList, source.rowTypeInfo, key, pos)
  }

  def unapply(m: MethodElement) = m match {
    case MethodElement(
          QueryMethod.AGGREGATE_BY_UNTYPED,
          argList,
          projectedType: TypeInfo[_],
          key,
          pos: MethodPosition) =>
      argList match {
        case List(
              MethodArg(_, src: MultiRelationElement),
              MethodArg(_, ExpressionListElement(groupByParametersList)),
              MethodArg(_, ExpressionListElement(accumulatorBuildersList))) =>
          val groupByParameters =
            groupByParametersList.map(_.asInstanceOf[ConstValueElement].value.asInstanceOf[String]).toSet
          val accumulatorBuilders =
            accumulatorBuildersList.map(_.asInstanceOf[ConstValueElement].value.asInstanceOf[UntypedAccumulatorBuilder])
          Some((src, groupByParameters, accumulatorBuilders, key, pos))
        case _ => None
      }
    case _ => None
  }
}

object FilterMethodElement {

  def apply(
      src: MultiRelationElement,
      srcName: String,
      expression: RelationElement,
      key: RelationKey[_],
      pos: MethodPosition) = {
    val args = new mutable.ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg(srcName, src)
    args += new MethodArg(srcName, expression)
    new MethodElement(QueryMethod.WHERE, args.toList, src.projectedType(), key, pos)
  }

  def unapply(m: MethodElement) = m match {
    case MethodElement(
          QueryMethod.WHERE,
          List(
            MethodArg(srcName: String, src: MultiRelationElement),
            MethodArg(_, expressionList: ExpressionListElement)),
          typeInfo: TypeInfo[_],
          key,
          pos: MethodPosition) =>
      Some((src, srcName, expressionList, key, pos))
    case _ => None
  }
}

object ExtendTypedMethodElement {

  def apply[RowType](
      source: MultiRelationElement,
      extensionLambdas: ExpressionListElement,
      extensionValues: ExpressionListElement,
      extensionType: TypeInfo[_],
      baseType: TypeInfo[_],
      resultType: TypeInfo[_],
      key: RelationKey[RowType],
      pos: MethodPosition) = {
    val args = new mutable.ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg("src", source)
    args += new MethodArg("extensionLambdas", extensionLambdas)
    args += new MethodArg("extensionValues", extensionValues)
    args += new MethodArg("baseType", new ConstValueElement(baseType))
    args += new MethodArg("extensionType", new ConstValueElement(extensionType))
    new MethodElement(QueryMethod.EXTEND_TYPED, args.toList, resultType, key, pos)
  }

  def unapply(m: MethodElement) = m match {
    case MethodElement(
          QueryMethod.EXTEND_TYPED,
          List(
            src,
            MethodArg(_, ExpressionListElement(extensionLambdas: List[_])),
            MethodArg(_, ExpressionListElement(extensionValues: List[_])),
            MethodArg(_, ConstValueElement(baseType: TypeInfo[_], _)),
            MethodArg(_, ConstValueElement(extensionType: TypeInfo[_], _))),
          resultType: TypeInfo[_],
          key: RelationKey[_],
          pos: MethodPosition) =>
      Some(
        (
          src,
          extensionLambdas.asInstanceOf[List[FuncElement]],
          extensionValues.asInstanceOf[List[ConstValueElement]],
          baseType.cast[Any],
          extensionType.cast[Any],
          resultType,
          key,
          pos))
    case _ => None
  }
}

object ReplaceMethodElement {

  def apply[RowType](
      source: MultiRelationElement,
      extensionLambdas: ExpressionListElement,
      extensionValues: ExpressionListElement,
      replacedType: TypeInfo[_],
      baseType: TypeInfo[_],
      key: RelationKey[RowType],
      pos: MethodPosition) = {
    val args = new mutable.ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg("src", source)
    args += new MethodArg("replacementLambdas", extensionLambdas)
    args += new MethodArg("replacementValues", extensionValues)
    args += new MethodArg("baseType", new ConstValueElement(baseType))
    args += new MethodArg("replacedType", new ConstValueElement(replacedType))
    new MethodElement(QueryMethod.REPLACE, args.toList, baseType, key, pos)
  }

  def unapply(m: MethodElement) = m match {
    case MethodElement(
          QueryMethod.REPLACE,
          List(
            src,
            MethodArg(_, ExpressionListElement(replacementLambdas: List[_])),
            MethodArg(_, ExpressionListElement(replacementValues: List[_])),
            MethodArg(_, ConstValueElement(baseType: TypeInfo[_], _)),
            MethodArg(_, ConstValueElement(replacedType: TypeInfo[_], _))),
          resultType: TypeInfo[_],
          key: RelationKey[_],
          pos: MethodPosition) =>
      Some(
        (
          src,
          replacementLambdas.asInstanceOf[List[FuncElement]],
          replacementValues.asInstanceOf[List[ConstValueElement]],
          baseType.cast[Any],
          replacedType.cast[Any],
          key,
          pos))
    case _ => None
  }
}

object AnyJoinMethodElement {
  def unapply(m: MethodElement) = m match {
    case MethodElement(
          method,
          MethodArg(leftName, left: MultiRelationElement) :: MethodArg(
            rightName,
            right: MultiRelationElement) :: otherArgs,
          rowType,
          key,
          pos: MethodPosition) if QueryMethod.isJoin(method) =>
      Some((leftName, left, rightName, right, rowType, key))
    case _ => None
  }
}

object InnerJoinMethodElement {

  def apply[RowType](
      left: MultiRelationElement,
      leftName: String,
      right: MultiRelationElement,
      rightName: String,
      leftKeySelector: ExpressionListElement,
      rightKeySelector: ExpressionListElement,
      targetName: String,
      projectionSelector: ExpressionListElement,
      key: RelationKey[RowType],
      projectedType: TypeInfo[_],
      pos: MethodPosition): MethodElement = {

    val args = new mutable.ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg(leftName, left)
    args += new MethodArg(rightName, right)
    args += new MethodArg(leftName, leftKeySelector)
    args += new MethodArg(rightName, rightKeySelector)
    args += new MethodArg(targetName, projectionSelector)

    new MethodElement(QueryMethod.INNER_JOIN, args.toList, projectedType, key, pos)
  }

  def unapply(m: MethodElement) = m match {

    case MethodElement(
          QueryMethod.INNER_JOIN,
          List(
            MethodArg(leftName, left: MultiRelationElement),
            MethodArg(rightName, right: MultiRelationElement),
            MethodArg(_, ExpressionListElement(leftKeySelector)),
            MethodArg(_, ExpressionListElement(rightKeySelector)),
            selector: MethodArg[RelationElement]),
          projectedType,
          key,
          pos: MethodPosition) =>
      Some(
        (
          leftName,
          left,
          rightName,
          right,
          leftKeySelector.asInstanceOf[List[FuncElement]],
          rightKeySelector.asInstanceOf[List[FuncElement]],
          selector,
          projectedType,
          key,
          pos))
    case _ => None
  }
}

object NaturalInnerJoinMethodElement {

  def apply[RowType](
      left: MultiRelationElement,
      leftName: String,
      right: MultiRelationElement,
      rightName: String,
      keySelector: ExpressionListElement,
      key: RelationKey[RowType],
      projectedType: TypeInfo[_],
      pos: MethodPosition): MethodElement = {

    val args = new mutable.ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg(leftName, left)
    args += new MethodArg(rightName, right)
    args += new MethodArg("keySelector", keySelector)

    new MethodElement(QueryMethod.NATURAL_INNER_JOIN, args.toList, projectedType, key, pos)
  }

  def unapply(m: MethodElement) = m match {
    case MethodElement(
          QueryMethod.NATURAL_INNER_JOIN,
          List(
            MethodArg(leftName, left: MultiRelationElement),
            MethodArg(rightName, right: MultiRelationElement),
            MethodArg(_, ExpressionListElement(keySelector))),
          projectedType,
          key,
          pos: MethodPosition) =>
      Some((leftName, left, rightName, right, keySelector.asInstanceOf[List[FuncElement]], projectedType, key, pos))
    case _ => None
  }
}

object OuterJoinMethodElement {

  def apply[RowType](
      left: MultiRelationElement,
      leftName: String,
      right: MultiRelationElement,
      rightName: String,
      leftKeySelector: ExpressionListElement,
      rightKeySelector: ExpressionListElement,
      targetName: String,
      defValueCreator1: RelationElement,
      defValueCreator2: RelationElement,
      projectionSelector: ExpressionListElement,
      method: QueryMethod,
      key: RelationKey[RowType],
      projectedType: TypeInfo[_],
      pos: MethodPosition): MethodElement = {

    val args = new mutable.ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg(leftName, left)
    args += new MethodArg(rightName, right)
    args += new MethodArg(leftName, leftKeySelector)
    args += new MethodArg(rightName, rightKeySelector)
    args += new MethodArg(targetName, projectionSelector)
    args += new MethodArg(leftName, defValueCreator1)
    args += new MethodArg(rightName, defValueCreator2)

    new MethodElement(method, args.toList, projectedType, key, pos)
  }

  def unapply(m: MethodElement) = m match {
    case MethodElement(
          method,
          List(
            MethodArg(leftName, left: MultiRelationElement),
            MethodArg(rightName, right: MultiRelationElement),
            MethodArg(_, ExpressionListElement(leftKeySelector)),
            MethodArg(_, ExpressionListElement(rightKeySelector)),
            selector: MethodArg[RelationElement],
            leftDefValueCreator: MethodArg[RelationElement],
            rightDefValueCreator: MethodArg[RelationElement]),
          projectedType,
          key,
          pos: MethodPosition)
        if (method == QueryMethod.LEFT_OUTER_JOIN || method == QueryMethod.RIGHT_OUTER_JOIN || method == QueryMethod.FULL_OUTER_JOIN) =>
      Some(
        (
          leftName,
          left,
          rightName,
          right,
          leftKeySelector.asInstanceOf[List[FuncElement]],
          rightKeySelector.asInstanceOf[List[FuncElement]],
          selector,
          leftDefValueCreator,
          rightDefValueCreator,
          method,
          projectedType,
          key,
          pos))
    case _ => None
  }
}

object NaturalOuterJoinMethodElement {

  def apply[RowType](
      left: MultiRelationElement,
      leftName: String,
      right: MultiRelationElement,
      rightName: String,
      keySelector: ExpressionListElement,
      defValueCreator1: RelationElement,
      defValueCreator2: RelationElement,
      method: QueryMethod,
      key: RelationKey[RowType],
      projectedType: TypeInfo[_],
      pos: MethodPosition): MethodElement = {

    val args = new mutable.ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg(leftName, left)
    args += new MethodArg(rightName, right)
    args += new MethodArg("keySelector", keySelector)
    args += new MethodArg(leftName, defValueCreator1)
    args += new MethodArg(rightName, defValueCreator2)

    new MethodElement(method, args.toList, projectedType, key, pos)
  }

  def unapply(m: MethodElement) = m match {
    case MethodElement(
          method,
          List(
            MethodArg(leftName, left: MultiRelationElement),
            MethodArg(rightName, right: MultiRelationElement),
            MethodArg(_, ExpressionListElement(keySelector)),
            leftDefValueCreator: MethodArg[RelationElement],
            rightDefValueCreator: MethodArg[RelationElement]),
          projectedType,
          key,
          pos: MethodPosition)
        if (method == QueryMethod.NATURAL_LEFT_OUTER_JOIN || method == QueryMethod.NATURAL_RIGHT_OUTER_JOIN || method == QueryMethod.NATURAL_FULL_OUTER_JOIN) =>
      Some(
        (
          leftName,
          left,
          rightName,
          right,
          keySelector.asInstanceOf[List[FuncElement]],
          leftDefValueCreator,
          rightDefValueCreator,
          method,
          projectedType,
          key,
          pos))
    case _ => None
  }

}

object GroupByMethodElement {

  def apply[RowType](
      source: MultiRelationElement,
      srcName: String,
      condition: ExpressionListElement,
      projectionType: TypeInfo[_],
      key: RelationKey[RowType],
      pos: MethodPosition) = {
    val args = new mutable.ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg("src", source)
    args += new MethodArg("condition", condition)
    new MethodElement(QueryMethod.GROUP_BY, args.toList, projectionType, key, pos)
  }

  def unapply(m: MethodElement) = m match {
    case MethodElement(
          QueryMethod.GROUP_BY,
          List(MethodArg(srcName: String, src: MultiRelationElement), MethodArg(_, condition: ExpressionListElement)),
          projectedType: TypeInfo[_],
          key,
          pos: MethodPosition) =>
      Some((srcName, src, condition, projectedType, key, pos))
    case _ => None
  }
}

object OutputMethodElement {

  def apply[RowType](
      source: MultiRelationElement,
      srcName: String,
      projectionSelector: ExpressionListElement,
      targetName: String,
      projectedType: TypeInfo[_],
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement = {

    val args = new mutable.ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg(srcName, source)
    args += new MethodArg(targetName, projectionSelector)
    new MethodElement(QueryMethod.OUTPUT, args.toList, projectedType, key, pos)
  }

  def unapply(m: MethodElement) = m match {
    case MethodElement(QueryMethod.OUTPUT, argList, projectedType: TypeInfo[_], key, pos: MethodPosition) =>
      argList match {
        case List(MethodArg(_, src: MultiRelationElement), MethodArg(_, projectionSelector: ExpressionListElement)) =>
          Some((src, projectionSelector, None, projectedType, key, pos))
        case List(
              MethodArg(_, src: MultiRelationElement),
              MethodArg(_, projectionSelector: ExpressionListElement),
              MethodArg(_, grpSourceAcc: RelationElement)) =>
          Some((src, projectionSelector, Some(grpSourceAcc), projectedType, key, pos))
        case as => throw new MatchError(as)
      }
    case _ => None
  }
}

object SortByMethodElement {

  def apply[RowType](
      source: MultiRelationElement,
      srcName: String,
      condition: ExpressionListElement,
      ascOrders: ExpressionListElement,
      key: RelationKey[RowType],
      lambda: ExpressionListElement,
      pos: MethodPosition): MethodElement = {

    val args = new mutable.ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg(srcName, source)
    args += new MethodArg("condition", condition)
    args += new MethodArg("lambda", lambda)
    new MethodElement(QueryMethod.SORT, args.toList, source.projectedType(), key, pos)
  }

  def unapply(m: MethodElement) = m match {
    case MethodElement(
          QueryMethod.SORT,
          List(src, MethodArg(_, expr), MethodArg(_, lambda)),
          projectedType: TypeInfo[_],
          key,
          pos: MethodPosition) =>
      Some(
        (
          src.arg,
          expr.asInstanceOf[ExpressionListElement],
          lambda.asInstanceOf[ExpressionListElement],
          projectedType,
          key,
          pos))
    case _ => None
  }

}

object ExtractMethodElement {

  def apply[RowType](
      source: MultiRelationElement,
      srcName: String,
      condition: ExpressionListElement,
      numRows: ConstValueElement,
      key: RelationKey[RowType],
      lambda: ExpressionListElement,
      pos: MethodPosition): MethodElement = {
    val args = List[optimus.platform.relational.tree.MethodArg[RelationElement]](
      new MethodArg(srcName, source),
      new MethodArg("condition", condition),
      new MethodArg("numRows", numRows),
      new MethodArg("orderExtractionLambda", lambda))
    val res = new MethodElement(QueryMethod.EXTRACT, args, source.projectedType(), key, pos)
    res
  }

  def unapply(m: MethodElement) = m match {
    case MethodElement(
          QueryMethod.EXTRACT,
          List(src, condition, numRows, lambda),
          projectedType: TypeInfo[_],
          key,
          pos) =>
      Some(
        src.arg,
        condition.arg.asInstanceOf[ExpressionListElement],
        lambda.arg.asInstanceOf[ExpressionListElement],
        numRows.arg.asInstanceOf[ConstValueElement])
    case _ => None
  }
}

object FlatMapMethodElement {

  def apply[RowType](
      source: MultiRelationElement,
      srcName: String,
      flatMapSelector: ExpressionListElement,
      targetName: String,
      projectedType: TypeInfo[_],
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement = {

    val args = new mutable.ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg(srcName, source)
    args += new MethodArg(targetName, flatMapSelector)
    new MethodElement(QueryMethod.FLATMAP, args.toList, projectedType, key, pos)
  }

  def unapply(m: MethodElement) = m match {
    case MethodElement(QueryMethod.FLATMAP, argList, projectedType: TypeInfo[_], key, pos: MethodPosition) =>
      argList match {
        case List(MethodArg(_, src: MultiRelationElement), MethodArg(_, flatMapSelector: ExpressionListElement)) =>
          Some((src, flatMapSelector, None, projectedType, key, pos))
        case as => throw new MatchError(as)
      }
    case _ => None
  }
}

object AggregateByMethodElement {

  def apply[RowType](
      source: MultiRelationElement,
      groupKeyType: TypeInfo[_],
      groupValueType: TypeInfo[_],
      groupByTypedProjectionType: TypeInfo[_],
      groupByTypedKey: RelationKey[_],
      groupByTypedValueKey: RelationKey[_],
      groupMapValueProjectionType: TypeInfo[_],
      groupMapValueKey: RelationKey[_],
      aggregateValueType: TypeInfo[_],
      func: ConstValueElement,
      aggregateExpressionElement: RelationElement,
      projectionType: TypeInfo[_],
      key: RelationKey[_],
      pos: MethodPosition): MethodElement = {

    val args = new mutable.ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg("src", source)
    args += new MethodArg("groupKeyType", new ConstValueElement(groupKeyType))
    args += new MethodArg("groupValueType", new ConstValueElement(groupValueType))
    args += new MethodArg("aggregateValueType", new ConstValueElement(aggregateValueType))

    args += new MethodArg("groupByTypedValueKey", new ConstValueElement(groupByTypedValueKey))
    args += new MethodArg("groupByTypedProjectionType", new ConstValueElement(groupByTypedProjectionType))
    args += new MethodArg("groupByTypedKey", new ConstValueElement(groupByTypedKey))

    args += new MethodArg("groupMapValueProjectionType", new ConstValueElement(groupMapValueProjectionType))
    args += new MethodArg("groupMapValueKey", new ConstValueElement(groupMapValueKey))

    args += new MethodArg("aggregateByLambda", func)
    args += new MethodArg("aggregateExpressionElement", aggregateExpressionElement)

    new MethodElement(QueryMethod.AGGREGATE_BY, args.toList, projectionType, key, pos)
  }

  def unapply(m: MethodElement) = m match {
    case MethodElement(QueryMethod.AGGREGATE_BY, argList, projectedType: TypeInfo[_], key, pos: MethodPosition) =>
      argList match {
        case List(
              MethodArg(_, src: MultiRelationElement),
              MethodArg(_, ConstValueElement(groupKeyType: TypeInfo[_], _)),
              MethodArg(_, ConstValueElement(groupValueType: TypeInfo[_], _)),
              MethodArg(_, ConstValueElement(aggregateValueType: TypeInfo[_], _)),
              MethodArg(_, ConstValueElement(groupByTypedValueKey: RelationKey[_], _)),
              MethodArg(_, ConstValueElement(groupByTypedProjectionType: TypeInfo[_], _)),
              MethodArg(_, ConstValueElement(groupByTypedKey: RelationKey[_], _)),
              MethodArg(_, ConstValueElement(groupMapValueProjectionType: TypeInfo[_], _)),
              MethodArg(_, ConstValueElement(groupMapValueKey: RelationKey[_], _)),
              MethodArg(_, aggregateBy: ConstValueElement),
              MethodArg(_, aggregateExpressionElement)) =>
          Some(
            (
              src,
              groupKeyType,
              groupValueType,
              aggregateValueType,
              groupByTypedValueKey,
              groupByTypedProjectionType,
              groupByTypedKey,
              groupMapValueProjectionType,
              groupMapValueKey,
              aggregateBy,
              aggregateExpressionElement.asInstanceOf[AggregateExpressionElement],
              key,
              pos))
        case _ => None
      }
    case _ => None
  }
}

object UntypeMethodElement {

  def apply(
      source: MultiRelationElement,
      dynamicExpr: ExpressionListElement,
      projectedType: TypeInfo[_],
      key: RelationKey[_],
      pos: MethodPosition) = {
    val args = new mutable.ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg("src", source)
    args += new MethodArg("dynamicExpr", dynamicExpr)
    new MethodElement(QueryMethod.UNTYPE, args.toList, projectedType, key, pos)
  }

  def unapply(m: MethodElement) = m match {
    case MethodElement(
          QueryMethod.UNTYPE,
          List(MethodArg(_, src: MultiRelationElement), MethodArg(_, dynamicExpr: ExpressionListElement)),
          typeInfo: TypeInfo[_],
          key,
          pos: MethodPosition) =>
      Some((src, dynamicExpr, typeInfo, key, pos))
    case _ => None
  }
}
