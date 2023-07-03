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

import optimus.platform.RelationKey
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.tree.FuncCode.FuncCode

import scala.collection.mutable.ListBuffer

object LegacyElementFactory {
  def where[RowType](
      source: MultiRelationElement,
      srcName: String,
      condition: RelationElement,
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement = {
    FilterMethodElement(source, srcName, condition, key, pos)
  }

  def innerJoin[RowType](
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

    InnerJoinMethodElement(
      left,
      leftName,
      right,
      rightName,
      leftKeySelector,
      rightKeySelector,
      targetName,
      projectionSelector,
      key,
      projectedType,
      pos)
  }
  def naturalInnerJoin[RowType](
      left: MultiRelationElement,
      leftName: String,
      right: MultiRelationElement,
      rightName: String,
      keySelector: ExpressionListElement,
      key: RelationKey[RowType],
      projectedType: TypeInfo[_],
      pos: MethodPosition): MethodElement = {

    NaturalInnerJoinMethodElement(left, leftName, right, rightName, keySelector, key, projectedType, pos)
  }

  def outerJoin[RowType](
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

    OuterJoinMethodElement(
      left,
      leftName,
      right,
      rightName,
      leftKeySelector,
      rightKeySelector,
      targetName,
      defValueCreator1,
      defValueCreator2,
      projectionSelector,
      method,
      key,
      projectedType,
      pos
    )

  }
  def naturalOuterJoin[RowType](
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

    NaturalOuterJoinMethodElement(
      left,
      leftName,
      right,
      rightName,
      keySelector,
      defValueCreator1,
      defValueCreator2,
      method,
      key,
      projectedType,
      pos)

  }

  def leftOuterJoin[RowType](
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
      key: RelationKey[RowType],
      projectedType: TypeInfo[_]): MethodElement =
    OuterJoinMethodElement(
      left,
      leftName,
      right,
      rightName,
      leftKeySelector,
      rightKeySelector,
      targetName,
      defValueCreator1,
      defValueCreator2,
      projectionSelector,
      QueryMethod.LEFT_OUTER_JOIN,
      key,
      projectedType,
      MethodPosition.unknown
    )

  def naturalLeftOuterJoin[RowType](
      left: MultiRelationElement,
      leftName: String,
      right: MultiRelationElement,
      rightName: String,
      keySelector: ExpressionListElement,
      defValueCreator1: RelationElement,
      defValueCreator2: RelationElement,
      key: RelationKey[RowType],
      projectedType: TypeInfo[_],
      pos: MethodPosition): MethodElement =
    NaturalOuterJoinMethodElement(
      left,
      leftName,
      right,
      rightName,
      keySelector,
      defValueCreator1,
      defValueCreator2,
      QueryMethod.NATURAL_LEFT_OUTER_JOIN,
      key,
      projectedType,
      pos)

  def rightOuterJoin[RowType](
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
      key: RelationKey[RowType],
      projectedType: TypeInfo[_]): MethodElement =
    OuterJoinMethodElement(
      left,
      leftName,
      right,
      rightName,
      leftKeySelector,
      rightKeySelector,
      targetName,
      defValueCreator1,
      defValueCreator2,
      projectionSelector,
      QueryMethod.RIGHT_OUTER_JOIN,
      key,
      projectedType,
      MethodPosition.unknown
    )

  def naturalRightOuterJoin[RowType](
      left: MultiRelationElement,
      leftName: String,
      right: MultiRelationElement,
      rightName: String,
      keySelector: ExpressionListElement,
      defValueCreator1: RelationElement,
      defValueCreator2: RelationElement,
      key: RelationKey[RowType],
      projectedType: TypeInfo[_],
      pos: MethodPosition): MethodElement =
    NaturalOuterJoinMethodElement(
      left,
      leftName,
      right,
      rightName,
      keySelector,
      defValueCreator1,
      defValueCreator2,
      QueryMethod.NATURAL_RIGHT_OUTER_JOIN,
      key,
      projectedType,
      pos)

  def fullOuterJoin[RowType](
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
      key: RelationKey[RowType],
      projectedType: TypeInfo[_]): MethodElement =
    OuterJoinMethodElement(
      left,
      leftName,
      right,
      rightName,
      leftKeySelector,
      rightKeySelector,
      targetName,
      defValueCreator1,
      defValueCreator2,
      projectionSelector,
      QueryMethod.FULL_OUTER_JOIN,
      key,
      projectedType,
      MethodPosition.unknown
    )

  def naturalFullOuterJoin[RowType](
      left: MultiRelationElement,
      leftName: String,
      right: MultiRelationElement,
      rightName: String,
      keySelector: ExpressionListElement,
      defValueCreator1: RelationElement,
      defValueCreator2: RelationElement,
      key: RelationKey[RowType],
      projectedType: TypeInfo[_],
      pos: MethodPosition): MethodElement =
    NaturalOuterJoinMethodElement(
      left,
      leftName,
      right,
      rightName,
      keySelector,
      defValueCreator1,
      defValueCreator2,
      QueryMethod.NATURAL_FULL_OUTER_JOIN,
      key,
      projectedType,
      pos)

  def groupBy[RowType](
      source: MultiRelationElement,
      srcName: String,
      condition: ExpressionListElement,
      projectionType: TypeInfo[_],
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement =
    GroupByMethodElement[RowType](source, srcName, condition, projectionType, key, pos)

  def groupByTyped[RowType, ValueType](
      source: MultiRelationElement,
      groupKeyType: TypeInfo[_],
      valueType: TypeInfo[_],
      projectionType: TypeInfo[_],
      key: RelationKey[RowType],
      valueKey: RelationKey[ValueType],
      pos: MethodPosition): MethodElement = {

    val args = new ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg("src", source)
    args += new MethodArg("keyTypes", new ConstValueElement(groupKeyType))
    args += new MethodArg("valueTypes", new ConstValueElement(valueType))
    args += new MethodArg("valueKey", new ConstValueElement(valueKey))
    val res = new MethodElement(QueryMethod.GROUP_BY_TYPED, args.toList, projectionType, key, pos)
    res
  }

  def groupMapValues[RowType](
      source: MultiRelationElement,
      mapValueFuncNode: ConstValueElement,
      projectionType: TypeInfo[_],
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement = {

    val args = new ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg("src", source)
    args += new MethodArg("mapLamda", mapValueFuncNode)
    val res = new MethodElement(QueryMethod.GROUP_MAP_VALUES, args.toList, projectionType, key, pos)
    res
  }

  def aggregateBy(
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
    val args = new ListBuffer[MethodArg[RelationElement]]()

    AggregateByMethodElement(
      source,
      groupKeyType,
      groupValueType,
      groupByTypedProjectionType,
      groupByTypedKey,
      groupByTypedValueKey,
      groupMapValueProjectionType,
      groupMapValueKey,
      aggregateValueType,
      func,
      aggregateExpressionElement,
      projectionType,
      key,
      pos
    )
  }

  def groupAll[RowType](
      source: MultiRelationElement,
      projectionType: TypeInfo[_],
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement = {
    val args = new ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg("src", source)
    val res = new MethodElement(QueryMethod.GROUP_ALL, args.toList, projectionType, key, pos)
    res
  }

  def sort[RowType](
      source: MultiRelationElement,
      srcName: String,
      condition: ExpressionListElement,
      ascOrders: ExpressionListElement,
      key: RelationKey[RowType],
      lambda: ExpressionListElement,
      pos: MethodPosition): MethodElement = {

    SortByMethodElement(source, srcName, condition, ascOrders, key, lambda, pos)
  }

  def take[RowType](
      source: MultiRelationElement,
      offset: Int,
      numRows: Int,
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement = {
    val args = new ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg("src", source)
    args += new MethodArg("offset", new ConstValueElement(offset))
    args += new MethodArg("numRows", new ConstValueElement(numRows))
    val res = new MethodElement(QueryMethod.TAKE, args.toList, source.projectedType(), key, pos)
    res
  }

  def extract[RowType, B](
      source: MultiRelationElement,
      srcName: String,
      condition: ExpressionListElement,
      numRows: ConstValueElement,
      key: RelationKey[RowType],
      lambda: ExpressionListElement,
      pos: MethodPosition): MethodElement = {
    ExtractMethodElement(source, srcName, condition, numRows, key, lambda, pos)
  }

  def takeDistinct[RowType](
      source: MultiRelationElement,
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement = {
    val args = new ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg("src", source)
    val res = new MethodElement(QueryMethod.TAKE_DISTINCT, args.toList, source.projectedType(), key, pos)
    res
  }

  def output[RowType](
      source: MultiRelationElement,
      srcName: String,
      projectionSelector: ExpressionListElement,
      targetName: String,
      projectedType: TypeInfo[_],
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement = {
    OutputMethodElement(source, srcName, projectionSelector, targetName, projectedType, key, pos)
  }

  def flatMap[RowType](
      source: MultiRelationElement,
      srcName: String,
      flatMapSelector: ExpressionListElement,
      targetName: String,
      projectedType: TypeInfo[_],
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement = {

    FlatMapMethodElement(source, srcName, flatMapSelector, targetName, projectedType, key, pos)
  }

  def union[RowType](
      left: MultiRelationElement,
      leftName: String,
      right: MultiRelationElement,
      rightName: String,
      unionAll: Boolean,
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement = union(Seq(left, right), "inputs", unionAll, key, pos)

  def union[RowType](
      inputs: Seq[MultiRelationElement],
      inputsName: String,
      unionAll: Boolean,
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement = {
    val typ = inputs.head.projectedType()
    val args = new ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg(inputsName, new ExpressionListElement(inputs.toList, typ))
    args += new MethodArg("isUnionAll", new ConstValueElement(unionAll))

    val res = new MethodElement(QueryMethod.UNION, args.toList, typ, key, pos)
    res
  }

  def difference[RowType](
      left: MultiRelationElement,
      leftName: String,
      right: MultiRelationElement,
      rightName: String,
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement = {
    val args = new ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg(leftName, left)
    args += new MethodArg(rightName, right)
    val res = new MethodElement(QueryMethod.DIFFERENCE, args.toList, left.projectedType(), key, pos)
    res
  }

  def division[RowType](
      left: MultiRelationElement,
      leftName: String,
      right: MultiRelationElement,
      rightName: String,
      projectedType: TypeInfo[_],
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement = {
    val args = new ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg(leftName, left)
    args += new MethodArg(rightName, right)
    val res = new MethodElement(QueryMethod.DIVISION, args.toList, projectedType, key, pos)
    res
  }

  def extend[RowType](
      source: MultiRelationElement,
      extension: ExpressionListElement,
      column: String,
      projectedType: TypeInfo[_],
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement = {
    val args = new ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg("src", source)
    args += new MethodArg("extension", extension)
    args += new MethodArg("columnName", new ConstValueElement(column))
    val res = new MethodElement(QueryMethod.EXTEND, args.toList, projectedType, key, pos)
    res
  }

  def extendTyped[RowType](
      source: MultiRelationElement,
      extensionLambdas: ExpressionListElement,
      extensionValues: ExpressionListElement,
      extensionType: TypeInfo[_],
      baseType: TypeInfo[_],
      resultType: TypeInfo[_],
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement = {
    ExtendTypedMethodElement(source, extensionLambdas, extensionValues, extensionType, baseType, resultType, key, pos)
  }

  def replace[RowType](
      source: MultiRelationElement,
      extensionLambdas: ExpressionListElement,
      extensionValues: ExpressionListElement,
      replacedType: TypeInfo[_],
      baseType: TypeInfo[_],
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement = {
    ReplaceMethodElement(source, extensionLambdas, extensionValues, replacedType, baseType, key, pos)
  }

  def untype[RowType](
      source: MultiRelationElement,
      dynamicExpr: ExpressionListElement,
      projectedType: TypeInfo[_],
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement = {
    val args = new ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg("src", source)
    args += new MethodArg("dynamicExpr", dynamicExpr)
    val res = new MethodElement(QueryMethod.UNTYPE, args.toList, projectedType, key, pos)
    res
  }

  def cast[RowType](
      source: MultiRelationElement,
      castExpr: ExpressionListElement,
      projectedType: TypeInfo[_],
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement = {
    val args = new ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg("src", source)
    args += new MethodArg("castExpr", castExpr)
    val res = new MethodElement(QueryMethod.CAST, args.toList, projectedType, key, pos)
    res
  }

  def shape[RowType](
      source: MultiRelationElement,
      shapeExpr: ExpressionListElement,
      projectedType: TypeInfo[_],
      key: RelationKey[RowType],
      pos: MethodPosition): MethodElement = {
    val args = new ListBuffer[MethodArg[RelationElement]]()
    args += new MethodArg("src", source)
    args += new MethodArg("shapeExpr", shapeExpr)
    val res = new MethodElement(QueryMethod.SHAPE, args.toList, projectedType, key, pos)
    res
  }

  def aggrFunc(
      arg: RelationElement,
      funcCode: FuncCode,
      argType: TypeInfo[_],
      forceResType: TypeInfo[_]): FuncElement = {
    if (argType == null) throw new IllegalArgumentException("Expected relation type.")
    val resType: TypeInfo[_] = {
      if (forceResType != null) forceResType
      else {
        val isOption = classOf[Option[_]].isAssignableFrom(argType.clazz)
        funcCode match {
          case _ => throw new RelationalUnsupportedException("not supported aggregate function")
        }
      }
    }

    val method = new RuntimeMethodDescriptor(
      TypeInfo.ITERABLE,
      funcCode.toString,
      resType,
      List(TypeInfo.NOTHING),
      List(("Argument", argType)))
    val func = new FuncElement(new MethodCallee(method), List(arg), null)
    func
  }
}
