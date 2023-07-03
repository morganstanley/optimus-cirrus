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
package optimus.platform.relational.internal

import optimus.platform.relational.tree._
import optimus.platform.internal.ValueFetch
import optimus.platform.relational.tree.BinaryExpressionType._
import scala.jdk.CollectionConverters._
import optimus.platform.relational._

object ExpressionElementHelper {
  def executeExpression(element: RelationElement, row: Any): Any = element match {

    case UnaryExpressionElement(_, element, projectedType) => assign(element, row, projectedType)

    case BinaryExpressionElement(op, left, right, _) =>
      op match {
        case BinaryExpressionType.EQ => equals(executeExpression(left, row), executeExpression(right, row))
        case BinaryExpressionType.NE => notEquals(executeExpression(left, row), executeExpression(right, row))
        case BinaryExpressionType.LT => lt(executeExpression(left, row), executeExpression(right, row))
        case BinaryExpressionType.LE => le(executeExpression(left, row), executeExpression(right, row))
        case BinaryExpressionType.GT => gt(executeExpression(left, row), executeExpression(right, row))
        case BinaryExpressionType.GE => ge(executeExpression(left, row), executeExpression(right, row))
        case BinaryExpressionType.BOOLAND =>
          val l = executeExpression(left.asInstanceOf[BinaryExpressionElement], row)
          val r = executeExpression(right.asInstanceOf[BinaryExpressionElement], row)
          val tempL = l.asInstanceOf[Boolean]
          val tempR = r.asInstanceOf[Boolean]
          tempL && tempR
        case BinaryExpressionType.BOOLOR =>
          val l = executeExpression(left.asInstanceOf[BinaryExpressionElement], row)
          val r = executeExpression(right.asInstanceOf[BinaryExpressionElement], row)
          val tempL = l.asInstanceOf[Boolean]
          val tempR = r.asInstanceOf[Boolean]
          tempL || tempR
        case BinaryExpressionType.PLUS   => plus(executeExpression(left, row), executeExpression(right, row))
        case BinaryExpressionType.DIV    => div(executeExpression(left, row), executeExpression(right, row))
        case BinaryExpressionType.MINUS  => minus(executeExpression(left, row), executeExpression(right, row))
        case BinaryExpressionType.MUL    => mul(executeExpression(left, row), executeExpression(right, row))
        case BinaryExpressionType.MODULO => modulo(executeExpression(left, row), executeExpression(right, row))
        case BinaryExpressionType.ITEM_IS_IN =>
          item_is_in(executeExpression(left, row), executeExpression(right, row), negate = false)
        case BinaryExpressionType.ITEM_IS_NOT_IN =>
          item_is_in(executeExpression(left, row), executeExpression(right, row), negate = true)
        case _ =>
          throw new RuntimeException("not support in BinaryExpressionType")
      }
    case _ =>
      ValueFetch.getValue(row, element)
  }

  def getType(x: Any): Class[_] = x match {
    case _: Byte    => java.lang.Byte.TYPE
    case _: Short   => java.lang.Short.TYPE
    case _: Int     => java.lang.Integer.TYPE
    case _: Long    => java.lang.Long.TYPE
    case _: Float   => java.lang.Float.TYPE
    case _: Double  => java.lang.Double.TYPE
    case _: Char    => java.lang.Character.TYPE
    case _: Boolean => java.lang.Boolean.TYPE
    case _: Unit    => java.lang.Void.TYPE
    case any        => any.getClass
  }

  def assign(element: Any, row: Any, projectedType: TypeInfo[_]): Any = element match {
    case memberElement: MemberElement => executeExpression(memberElement, row)
    case ExpressionListElement(expressionList) => {
      val params = expressionList.map(expression => executeExpression(expression, row))
      val paramTypes = params.map(_.getClass)

      val x = projectedType.clazz.getConstructors

      // Find an appropriate constructor - same number of arguments and suitable types.
      val constructor = projectedType.clazz.getConstructors.find(c => {
        val constructorParamTypes = c.getParameterTypes
        (constructorParamTypes.length == params.size) && (constructorParamTypes zip params).forall {
          case (paramType, param) =>
            paramType.isAssignableFrom(if (paramType.isPrimitive) getType(param) else param.getClass)
        }
      })

      constructor match {
        case Some(projectionConstructor) =>
          projectionConstructor.newInstance(params.map(_.asInstanceOf[Object]): _*).asInstanceOf[projectedType.Type]
        case None => throw new RelationalException("Unable to find suitable constructor")
      }
    }
  }

  def item_is_in[T](t: T, r: Any, negate: Boolean): Any =
    (r match {
      case coll: java.util.Collection[_]    => coll.asScala
      case it: scala.collection.Iterable[_] => it
      case _ => throw new RelationalException("type " + r.getClass() + " is not a collection")
    }).exists(_ == t) ^ negate

  def modulo(x: Any, r: Any): Any = {
    if (x.isInstanceOf[java.lang.Double] || r.isInstanceOf[java.lang.Double]) {
      FuncModulo.evaluate(x.asInstanceOf[java.lang.Number].doubleValue, r.asInstanceOf[java.lang.Number].doubleValue)
    } else if (
      (x.isInstanceOf[java.lang.Float] && r.isInstanceOf[java.lang.Long]) || (x
        .isInstanceOf[java.lang.Long] && r.isInstanceOf[java.lang.Float])
    ) {
      FuncModulo.evaluate(x.asInstanceOf[java.lang.Number].doubleValue, r.asInstanceOf[java.lang.Number].doubleValue)
    } else if (x.isInstanceOf[java.lang.Float] || r.isInstanceOf[java.lang.Float]) {
      FuncModulo.evaluate(x.asInstanceOf[java.lang.Number].floatValue, r.asInstanceOf[java.lang.Number].floatValue)
    } else if (x.isInstanceOf[java.lang.Long] || r.isInstanceOf[java.lang.Long]) {
      FuncModulo.evaluate(x.asInstanceOf[java.lang.Number].longValue, r.asInstanceOf[java.lang.Number].longValue)
    } else if (x.isInstanceOf[java.lang.Integer] || r.isInstanceOf[java.lang.Integer]) {
      FuncModulo.evaluate(x.asInstanceOf[java.lang.Number].intValue, r.asInstanceOf[java.lang.Number].intValue)
    } else if (x.isInstanceOf[java.lang.Short] || r.isInstanceOf[java.lang.Short]) {
      FuncModulo.evaluate(x.asInstanceOf[java.lang.Number].shortValue, r.asInstanceOf[java.lang.Number].shortValue)
    } else if (x.isInstanceOf[java.lang.Byte] || r.isInstanceOf[java.lang.Byte]) {
      FuncModulo.evaluate(x.asInstanceOf[java.lang.Number].byteValue, r.asInstanceOf[java.lang.Number].byteValue)
    } else {
      throw new RelationalException("the type " + x.getClass() + " and " + r.getClass() + " is not the type for modulo")
    }
  }

  def minus(x: Any, r: Any): Any = {
    if (x.isInstanceOf[java.lang.Double] || r.isInstanceOf[java.lang.Double]) {
      FuncMinus.evaluate(x.asInstanceOf[java.lang.Number].doubleValue, r.asInstanceOf[java.lang.Number].doubleValue)
    } else if (
      (x.isInstanceOf[java.lang.Float] && r.isInstanceOf[java.lang.Long]) || (x
        .isInstanceOf[java.lang.Long] && r.isInstanceOf[java.lang.Float])
    ) {
      FuncMinus.evaluate(x.asInstanceOf[java.lang.Number].doubleValue, r.asInstanceOf[java.lang.Number].doubleValue)
    } else if (x.isInstanceOf[java.lang.Float] || r.isInstanceOf[java.lang.Float]) {
      FuncMinus.evaluate(x.asInstanceOf[java.lang.Number].floatValue, r.asInstanceOf[java.lang.Number].floatValue)
    } else if (x.isInstanceOf[java.lang.Long] || r.isInstanceOf[java.lang.Long]) {
      FuncMinus.evaluate(x.asInstanceOf[java.lang.Number].longValue, r.asInstanceOf[java.lang.Number].longValue)
    } else if (x.isInstanceOf[java.lang.Integer] || r.isInstanceOf[java.lang.Integer]) {
      FuncMinus.evaluate(x.asInstanceOf[java.lang.Number].intValue, r.asInstanceOf[java.lang.Number].intValue)
    } else if (x.isInstanceOf[java.lang.Short] || r.isInstanceOf[java.lang.Short]) {
      FuncMinus.evaluate(x.asInstanceOf[java.lang.Number].shortValue, r.asInstanceOf[java.lang.Number].shortValue)
    } else if (x.isInstanceOf[java.lang.Byte] || r.isInstanceOf[java.lang.Byte]) {
      FuncMinus.evaluate(x.asInstanceOf[java.lang.Number].byteValue, r.asInstanceOf[java.lang.Number].byteValue)
    } else {
      throw new RelationalException("the type " + x.getClass() + " and " + r.getClass() + " is not the type for minus")
    }
  }

  def mul(x: Any, r: Any): Any = {
    if (x.isInstanceOf[java.lang.Double] || r.isInstanceOf[java.lang.Double]) {
      FuncMultiply.evaluate(x.asInstanceOf[java.lang.Number].doubleValue, r.asInstanceOf[java.lang.Number].doubleValue)
    } else if (
      (x.isInstanceOf[java.lang.Float] && r.isInstanceOf[java.lang.Long]) || (x
        .isInstanceOf[java.lang.Long] && r.isInstanceOf[java.lang.Float])
    ) {
      FuncMultiply.evaluate(x.asInstanceOf[java.lang.Number].doubleValue, r.asInstanceOf[java.lang.Number].doubleValue)
    } else if (x.isInstanceOf[java.lang.Float] || r.isInstanceOf[java.lang.Float]) {
      FuncMultiply.evaluate(x.asInstanceOf[java.lang.Number].floatValue, r.asInstanceOf[java.lang.Number].floatValue)
    } else if (x.isInstanceOf[java.lang.Long] || r.isInstanceOf[java.lang.Long]) {
      FuncMultiply.evaluate(x.asInstanceOf[java.lang.Number].longValue, r.asInstanceOf[java.lang.Number].longValue)
    } else if (x.isInstanceOf[java.lang.Integer] || r.isInstanceOf[java.lang.Integer]) {
      FuncMultiply.evaluate(x.asInstanceOf[java.lang.Number].intValue, r.asInstanceOf[java.lang.Number].intValue)
    } else if (x.isInstanceOf[java.lang.Short] || r.isInstanceOf[java.lang.Short]) {
      FuncMultiply.evaluate(x.asInstanceOf[java.lang.Number].shortValue, r.asInstanceOf[java.lang.Number].shortValue)
    } else if (x.isInstanceOf[java.lang.Byte] || r.isInstanceOf[java.lang.Byte]) {
      FuncMultiply.evaluate(x.asInstanceOf[java.lang.Number].byteValue, r.asInstanceOf[java.lang.Number].byteValue)
    } else {
      throw new RelationalException("the type " + x.getClass() + " and " + r.getClass() + " is not the type for mul")
    }
  }

  def div(x: Any, r: Any): Any = {
    FuncDivide.evaluate(x.asInstanceOf[java.lang.Number].doubleValue, r.asInstanceOf[java.lang.Number].doubleValue)
  }

  def plus(x: Any, r: Any): Any = {
    if (x.isInstanceOf[java.lang.Double] || r.isInstanceOf[java.lang.Double]) {
      FuncPlus.evaluate(x.asInstanceOf[java.lang.Number].doubleValue, r.asInstanceOf[java.lang.Number].doubleValue)
    } else if (
      (x.isInstanceOf[java.lang.Float] && r.isInstanceOf[java.lang.Long]) || (x
        .isInstanceOf[java.lang.Long] && r.isInstanceOf[java.lang.Float])
    ) {
      FuncPlus.evaluate(x.asInstanceOf[java.lang.Number].doubleValue, r.asInstanceOf[java.lang.Number].doubleValue)
    } else if (x.isInstanceOf[java.lang.Float] || r.isInstanceOf[java.lang.Float]) {
      FuncPlus.evaluate(x.asInstanceOf[java.lang.Number].floatValue, r.asInstanceOf[java.lang.Number].floatValue)
    } else if (x.isInstanceOf[java.lang.Long] || r.isInstanceOf[java.lang.Long]) {
      FuncPlus.evaluate(x.asInstanceOf[java.lang.Number].longValue, r.asInstanceOf[java.lang.Number].longValue)
    } else if (x.isInstanceOf[java.lang.Integer] || r.isInstanceOf[java.lang.Integer]) {
      FuncPlus.evaluate(x.asInstanceOf[java.lang.Number].intValue, r.asInstanceOf[java.lang.Number].intValue)
    } else if (x.isInstanceOf[java.lang.Short] || r.isInstanceOf[java.lang.Short]) {
      FuncPlus.evaluate(x.asInstanceOf[java.lang.Number].shortValue, r.asInstanceOf[java.lang.Number].shortValue)
    } else if (x.isInstanceOf[java.lang.Byte] || r.isInstanceOf[java.lang.Byte]) {
      FuncPlus.evaluate(x.asInstanceOf[java.lang.Number].byteValue, r.asInstanceOf[java.lang.Number].byteValue)
    } else {
      throw new RelationalException("the type " + x.getClass() + " and " + r.getClass() + " is not the type for plus")
    }
  }

  def equals[T](x: T, r: T): Boolean = {
    x == r
  }

  def notEquals[T](x: T, r: T): Boolean = {
    !(equals(x, r))
  }

  def lt[T](x: T, r: T): Boolean = {
    x match {
      case null => r == null
      case lc: java.lang.Comparable[T @unchecked] =>
        val res = lc.compareTo(r)
        res < 0
      case _ => false
    }
  }

  def le[T](x: T, r: T): Boolean = {
    x match {
      case null => r == null
      case lc: java.lang.Comparable[T @unchecked] =>
        val res = lc.compareTo(r)
        res <= 0
      case _ => false
    }
  }

  def gt[T](x: T, r: T): Boolean = {
    x match {
      case null => r == null
      case lc: java.lang.Comparable[T @unchecked] =>
        val res = lc.compareTo(r)
        res > 0
      case _ => false
    }
  }

  def ge[T](x: T, r: T): Boolean = {
    x match {
      case null => r == null
      case lc: java.lang.Comparable[T @unchecked] =>
        val res = lc.compareTo(r)
        res >= 0
      case _ => false
    }
  }
}
