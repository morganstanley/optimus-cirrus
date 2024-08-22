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
package optimus.platform.relational.reactive

import optimus.platform.pickling.DefaultPicklers
import optimus.platform.storable.PersistentEntity
import optimus.platform.relational.dal.StorableClassElement
import optimus.platform.relational.reactive.PersistentEntityCheckResult._
import optimus.platform.relational.tree.BinaryExpressionType
import optimus.platform.relational.tree.BinaryExpressionType._
import optimus.platform.relational.tree._

import optimus.scalacompat.collection.IterableLike

sealed trait PersistentEntityCondition extends Serializable {
  def check(pe: PersistentEntity): Boolean = {
    check(pe.serialized.properties) match {
      case MATCHED                      => true
      case UNMATCHED | MISSING_PROPERTY => false
    }
  }
  def check(props: Map[String, Any]): PersistentEntityCheckResult

  protected def getProp(props: Map[String, Any], names: Seq[String]) = {
    if (names ne Nil) names.tail.foldRight(props) { (nme, res) =>
      res.getOrElse(nme, MISSING_PROPERTY).asInstanceOf[Map[String, Any]]
    }
    else props
  }
}
final case class ParsedEqCondition(name: String, value: Any, names: Seq[String] = Nil)
    extends PersistentEntityCondition {
  def check(props: Map[String, Any]): PersistentEntityCheckResult = {
    val prop = getProp(props, names)
    prop.get(name) map { v =>
      PersistentEntityCheckResult.fromBoolean(v == value)
    } getOrElse MISSING_PROPERTY
  }
}
final case class ParsedNeCondition(name: String, value: Any, names: Seq[String] = Nil)
    extends PersistentEntityCondition {
  def check(props: Map[String, Any]): PersistentEntityCheckResult = {
    val prop = getProp(props, names)
    prop.get(name) map { v =>
      PersistentEntityCheckResult.fromBoolean(v != value)
    } getOrElse MISSING_PROPERTY
  }
}
final case class ParsedContainsCondition(name: String, value: IterableLike[_, _], names: Seq[String] = Nil)
    extends PersistentEntityCondition {
  def check(props: Map[String, Any]): PersistentEntityCheckResult = {
    val prop = getProp(props, names)
    prop.get(name) map { v =>
      PersistentEntityCheckResult.fromBoolean(value.toSeq.contains(v))
    } getOrElse MISSING_PROPERTY
  }
}
final case class ParsedTypedCondition(name: String, value: Any, op: BinaryExpressionType, names: Seq[String] = Nil)
    extends PersistentEntityCondition {
  def check(props: Map[String, Any]): PersistentEntityCheckResult = {
    val prop = getProp(props, names)
    prop.get(name) map { v =>
      PersistentEntityCheckResult.fromBoolean(rawCompare(v, op, value))
    } getOrElse MISSING_PROPERTY
  }

  private def rawCompare(left: Any, op: BinaryExpressionType, right: Any): Boolean = {
    (left, right) match {
      case (ln: Number, rn: Number) =>
        // Coerce numbers and do the comparison
        (left, right) match {
          case (_: Double, _) | (_, _: Double) => typedCompare(ln.doubleValue, op, rn.doubleValue)
          case (_: Float, _) | (_, _: Float)   => typedCompare(ln.floatValue, op, rn.floatValue)
          case (_: Long, _) | (_, _: Long)     => typedCompare(ln.longValue, op, rn.longValue)
          case (_: Int, _) | (_, _: Int)       => typedCompare(ln.intValue, op, rn.intValue)
          case (_: Short, _) | (_, _: Short)   => typedCompare(ln.shortValue, op, rn.shortValue)
          case (_: Byte, _) | (_, _: Byte)     => typedCompare(ln.byteValue, op, rn.byteValue)
          case _ =>
            throw new UnsupportedOperation(
              s"Unsupported comparison: ($left: ${left.getClass}) $op ($right: ${right.getClass})")
        }
      case _ =>
        throw new UnsupportedOperation(
          s"Unsupported comparison: ($left: ${left.getClass}) $op ($right: ${right.getClass})")
    }
  }

  private def typedCompare[T](left: T, op: BinaryExpressionType, right: T)(implicit ord: Ordering[T]): Boolean = {
    import ord._
    op match {
      case EQ => left == right
      case NE => left != right
      case LT => left < right
      case LE => left <= right
      case GT => left > right
      case GE => left >= right
      case _  => throw new UnsupportedOperation(op.toString)
    }
  }
}
final case class BoolAndCondition(c1: PersistentEntityCondition, c2: PersistentEntityCondition)
    extends PersistentEntityCondition {
  def check(props: Map[String, Any]): PersistentEntityCheckResult = {
    val base = c1.check(props)
    if (base == UNMATCHED) base else base.and(c2.check(props))
  }
}
final case class BoolOrCondition(c1: PersistentEntityCondition, c2: PersistentEntityCondition)
    extends PersistentEntityCondition {
  def check(props: Map[String, Any]): PersistentEntityCheckResult = {
    val base = c1.check(props)
    if (base == MATCHED) base else base.or(c2.check(props))
  }
}

object PersistentEntityCondition extends ExpressionNormalizer {

  def parseCondition(expression: RelationElement): PersistentEntityCondition = parseCondition(expression, None)
  def parseCondition(
      expression: RelationElement,
      op: BinaryExpressionType.Value,
      that: RelationElement): PersistentEntityCondition = parseCondition(expression, Some((op, that)))
  def parseCondition(
      expression: RelationElement,
      operand: Option[(BinaryExpressionType.Value, RelationElement)]): PersistentEntityCondition = {
    expression match {
      case b: BinaryExpressionElement =>
        b.op match {
          case BOOLAND | BOOLOR =>
            val exprParsed = parseCondition(b.left, b.op, b.right)
            combine(exprParsed, parseOperand(operand))
          case EQ | NE | LT | LE | GT | GE =>
            val (left, op, right) = normalize(b.left, b.op, b.right)
            left.instanceProvider match {
              case s: StorableClassElement => // direct property access
                val pickler = s.classInfo.propertyMetadata(left.memberName).pickler
                parseMemberComparison(left, b.op, pickled(right, pickler))
              case _ => // indirectly property access (a.b.c)
                val prop = MemberProperty(left)
                parseMemberComparison(prop, b.op, pickled(right, prop.pickler))
            }
          case ITEM_IS_IN =>
            (b.left, b.right) match {
              case (left: MemberElement, right: ConstValueElement) =>
                left.instanceProvider match {
                  case s: StorableClassElement => // direct property access
                    val pickler = s.classInfo.propertyMetadata(left.memberName).pickler
                    parseMemberContains(left, pickled(right, DefaultPicklers.iterablePickler(pickler)))
                  case _ => // indirectly property access (a.b.c)
                    val prop = MemberProperty(left)
                    parseMemberContains(prop, pickled(right, DefaultPicklers.iterablePickler(prop.pickler)))
                }
              case _ =>
                throw new UnsupportedOperation("Wrong lhs or rhs type for contains FuncElement")
            }
          case _ =>
            throw new IllegalArgumentException(s"Unknown binary expression type: ${b.op}")
        }
      case f: FuncElement if f.callee.name == FunctionNames.IN =>
        (f.arguments.head, f.instance) match {
          case (left: MemberElement, right: ConstValueElement) =>
            left.instanceProvider match {
              case s: StorableClassElement => // direct property access
                val pickler = s.classInfo.propertyMetadata(left.memberName).pickler
                parseMemberContains(left, pickled(right, DefaultPicklers.iterablePickler(pickler)))
              case _ => // indirectly property access (a.b.c)
                val prop = MemberProperty(left)
                parseMemberContains(prop, pickled(right, DefaultPicklers.iterablePickler(prop.pickler)))
            }
          case _ =>
            throw new UnsupportedOperation("Wrong lhs or rhs type for contains FuncElement")
        }
      case e =>
        throw new UnsupportedOperation(s"Only binary expression is supported, found $e")
    }
  }

  private def parseOperand(operand: Option[(BinaryExpressionType.Value, RelationElement)])
      : Option[(BinaryExpressionType, PersistentEntityCondition)] = {
    operand map { case (op, expression) =>
      (op, parseCondition(expression))
    }
  }

  private def combine(
      base: PersistentEntityCondition,
      operand: Option[(BinaryExpressionType, PersistentEntityCondition)]): PersistentEntityCondition = {
    val computed = operand map { case (op: BinaryExpressionType, other: PersistentEntityCondition) =>
      op match {
        case BOOLAND => BoolAndCondition(base, other)
        case BOOLOR  => BoolOrCondition(base, other)
        case op      => throw new UnsupportedOperation(s"Unknown binary expression type: $op")
      }
    }
    computed.getOrElse(base)
  }

  private def parseMemberComparison(
      left: MemberElement,
      op: BinaryExpressionType,
      value: Any): PersistentEntityCondition = {
    val name = left.memberName
    op match {
      case EQ                => ParsedEqCondition(name, value)
      case NE                => ParsedNeCondition(name, value)
      case LT | LE | GT | GE => ParsedTypedCondition(name, value, op)
      case _                 => throw new IllegalArgumentException(s"$op is not supported")
    }
  }

  private def parseMemberComparison(
      left: MemberProperty,
      op: BinaryExpressionType,
      value: Any): PersistentEntityCondition = {
    val names = left.names
    op match {
      case EQ                => ParsedEqCondition(left.name, value, names)
      case NE                => ParsedNeCondition(left.name, value, names)
      case LT | LE | GT | GE => ParsedTypedCondition(left.name, value, op, names)
      case _                 => throw new IllegalArgumentException(s"$op is not supported")
    }
  }

  private def parseMemberContains(left: MemberElement, value: Any): PersistentEntityCondition = {
    val name = left.memberName
    value match {
      case it: IterableLike[_, _] => ParsedContainsCondition(name, it)
    }
  }

  private def parseMemberContains(left: MemberProperty, value: Any): PersistentEntityCondition = {
    val names = left.names
    value match {
      case it: IterableLike[_, _] => ParsedContainsCondition(left.name, it, names)
    }
  }
}
