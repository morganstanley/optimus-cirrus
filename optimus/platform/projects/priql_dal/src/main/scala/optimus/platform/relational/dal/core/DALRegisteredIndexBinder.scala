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
package optimus.platform.relational.dal.core

import optimus.platform.relational.dal.core.DALReferenceReducer.DALReferenceBinder
import optimus.platform.relational.data.mapping.MemberInfo
import optimus.platform.relational.data.mapping.QueryMapper
import optimus.platform.relational.data.tree.ColumnElement
import optimus.platform.relational.data.tree.ColumnType
import optimus.platform.relational.tree.BinaryExpressionType._
import optimus.platform.relational.tree._

import java.time._

object DALRegisteredIndexBinder extends DALQueryBinder {

  def bind(mapper: QueryMapper, e: RelationElement): RelationElement = {
    new DALRegisteredIndexBinder(mapper, e).bind(e)
  }

  def handleFuncCall(
      func: FuncElement,
      defaultHandler: FuncElement => RelationElement
  ): RelationElement = {

    val default = defaultHandler(func)

    def dateExprElem(methodName: String, c: ColumnElement, v: ConstValueElement, inverse: Boolean) = {
      methodName match {
        case "isAfter" if inverse  => ElementFactory.greaterThan(c, v)
        case "isAfter"             => ElementFactory.lessThan(c, v)
        case "isBefore" if inverse => ElementFactory.lessThan(c, v)
        case "isBefore"            => ElementFactory.greaterThan(c, v)
        case "isEqual"             => ElementFactory.equal(c, v)
        case _                     => default
      }
    }

    def primitiveExprElem(op: BinaryExpressionType, c: ColumnElement, v: ConstValueElement, inverse: Boolean) = {
      op match {
        case LT if inverse => ElementFactory.greaterThan(c, v)
        case LT            => ElementFactory.lessThan(c, v)
        case LE if inverse => ElementFactory.greaterThanOrEqual(c, v)
        case LE            => ElementFactory.lessThanOrEqual(c, v)
        case GT if inverse => ElementFactory.lessThan(c, v)
        case GT            => ElementFactory.greaterThan(c, v)
        case GE if inverse => ElementFactory.lessThanOrEqual(c, v)
        case GE            => ElementFactory.greaterThanOrEqual(c, v)
        case _             => default
      }
    }

    default match {
      case FuncElement(mc: MethodCallee, List(c: ColumnElement), v: ConstValueElement)
          if isNonUniqueIndex(c) && isValidDateTimeExpr(mc) =>
        dateExprElem(mc.method.name, c, v, inverse = false)
      case FuncElement(mc: MethodCallee, List(v: ConstValueElement), c: ColumnElement)
          if isNonUniqueIndex(c) && isValidDateTimeExpr(mc) =>
        dateExprElem(mc.method.name, c, v, inverse = true)
      case FuncElement(mc: MethodCallee, List(LambdaElement(_, body, Seq(lp))), c: ColumnElement)
          if isNonUniqueIndex(c) && mc.method.declaringType <:< classOf[Iterable[_]] =>
        (mc.method.name, body) match {
          case ("exists", BinaryExpressionElement(op, p: ParameterElement, const: ConstValueElement, _)) if p eq lp =>
            primitiveExprElem(op, c, const, inverse = false)
          case ("exists", BinaryExpressionElement(op, const: ConstValueElement, p: ParameterElement, _)) if p eq lp =>
            primitiveExprElem(op, c, const, inverse = true)
          case ("exists", FuncElement(mc: MethodCallee, List(arg), inst)) if isValidDateTimeExpr(mc) =>
            (arg, inst) match {
              case (p: ParameterElement, const: ConstValueElement) if p eq lp =>
                dateExprElem(mc.method.name, c, const, inverse = false)
              case (const: ConstValueElement, p: ParameterElement) if p eq lp =>
                dateExprElem(mc.method.name, c, const, inverse = true)
              case _ => default
            }
          case _ => default
        }
      case FuncElement(mc: MethodCallee, List(LambdaElement(_, body, Seq(lp))), c: ColumnElement)
          if isNonUniqueIndex(c) && TypeInfo.isOption(mc.method.declaringType) =>
        (mc.method.name, body) match {
          case ("exists", BinaryExpressionElement(op, p: ParameterElement, const: ConstValueElement, _)) if p eq lp =>
            val optType = TypeInfo(classOf[Option[_]], const.rowTypeInfo)
            val v = ElementFactory.constant(Some(const.value), optType)
            primitiveExprElem(op, c, v, inverse = false)
          case ("exists", BinaryExpressionElement(op, const: ConstValueElement, p: ParameterElement, _)) if p eq lp =>
            val optType = TypeInfo(classOf[Option[_]], const.rowTypeInfo)
            val v = ElementFactory.constant(Some(const.value), optType)
            primitiveExprElem(op, c, v, inverse = true)
          case ("exists", FuncElement(mc: MethodCallee, List(arg), inst)) if isValidDateTimeExpr(mc) =>
            (arg, inst) match {
              case (p: ParameterElement, const: ConstValueElement) if p eq lp =>
                val optType = TypeInfo(classOf[Option[_]], const.rowTypeInfo)
                val v = ElementFactory.constant(Some(const.value), optType)
                dateExprElem(mc.method.name, c, v, inverse = false)
              case (const: ConstValueElement, p: ParameterElement) if p eq lp =>
                val optType = TypeInfo(classOf[Option[_]], const.rowTypeInfo)
                val v = ElementFactory.constant(Some(const.value), optType)
                dateExprElem(mc.method.name, c, v, inverse = true)
              case _ => default
            }
          case _ => default
        }
      case _ => default
    }
  }

  def handleBinaryExpression(
      element: BinaryExpressionElement,
      defaultHandler: BinaryExpressionElement => RelationElement
  ): RelationElement = {
    val defaultElem = defaultHandler(element)
    defaultElem match {
      // This is to allow "!dt.isAfter(someDt)" and "!dt.isBefore(someDt)" into <= and >=
      // expressions respectively, where "dt" field is registered @indexed.
      case BinaryExpressionElement(
            EQ,
            BinaryExpressionElement(GT, c: ColumnElement, v: ConstValueElement, _),
            ConstValueElement(false, _),
            _) if isNonUniqueIndex(c) =>
        ElementFactory.lessThanOrEqual(c, v)
      case BinaryExpressionElement(
            EQ,
            BinaryExpressionElement(GT, v: ConstValueElement, c: ColumnElement, _),
            ConstValueElement(false, _),
            _) if isNonUniqueIndex(c) =>
        ElementFactory.lessThanOrEqual(c, v)
      case BinaryExpressionElement(
            EQ,
            BinaryExpressionElement(LT, c: ColumnElement, v: ConstValueElement, _),
            ConstValueElement(false, _),
            _) if isNonUniqueIndex(c) =>
        ElementFactory.greaterThanOrEqual(c, v)
      case BinaryExpressionElement(
            EQ,
            BinaryExpressionElement(LT, v: ConstValueElement, c: ColumnElement, _),
            ConstValueElement(false, _),
            _) if isNonUniqueIndex(c) =>
        ElementFactory.greaterThanOrEqual(c, v)
      case _ => defaultElem
    }
  }

  private def isNonUniqueIndex(column: ColumnElement): Boolean = column.columnInfo.columnType match {
    case ColumnType.Index => true // we don't support registered index on keys and unique indexes
    case _                => false
  }

  private def isValidDateTimeExpr(mc: MethodCallee): Boolean = {
    if (mc.method != null) {
      val desc = mc.method
      val declType = desc.declaringType
      if (
        declType <:< classOf[LocalDate] ||
        declType <:< classOf[LocalTime] ||
        declType <:< classOf[OffsetTime] ||
        declType <:< classOf[ZonedDateTime]
      ) desc.name match {
        case "isAfter" | "isBefore" => true
        case _                      => false
      }
      else false
    } else false
  }
}

class DALRegisteredIndexBinder(mapper: QueryMapper, root: RelationElement) extends DALBinder(mapper, root) {
  override def handleFuncCall(func: FuncElement): RelationElement =
    DALRegisteredIndexBinder.handleFuncCall(func, super.handleFuncCall)
  override protected def handleBinaryExpression(element: BinaryExpressionElement): RelationElement =
    DALRegisteredIndexBinder.handleBinaryExpression(element, super.handleBinaryExpression)
}

object DALRegisteredIndexReferenceBinder extends DALQueryBinder {
  def bind(mapper: QueryMapper, e: RelationElement): RelationElement = {
    new DALRegisteredIndexReferenceBinder(mapper, e).bind(e)
  }
  override def bindMember(source: RelationElement, member: MemberInfo): RelationElement =
    DALReferenceBinder.bindMember(source, member)
}

class DALRegisteredIndexReferenceBinder(mapper: QueryMapper, root: RelationElement)
    extends DALReferenceBinder(mapper, root) {
  override def handleFuncCall(func: FuncElement): RelationElement =
    DALRegisteredIndexBinder.handleFuncCall(func, super.handleFuncCall)
  override protected def handleBinaryExpression(element: BinaryExpressionElement): RelationElement =
    DALRegisteredIndexBinder.handleBinaryExpression(element, super.handleBinaryExpression)
}
