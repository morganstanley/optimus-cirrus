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

import optimus.platform.cm.Knowable
import optimus.platform._
import optimus.platform.relational.aggregation.Count
import optimus.platform.relational.internal.RelationalUtils
import optimus.platform.relational.tree.QueryMethod._
import optimus.platform.relational.tree._

/**
 * For query: from(A).flatMap(a => from(B).filter(b => a.name == b.name))
 *
 * The LambdaReifier will convert flatMap's Function into a LambdaElement. Its inner 'filter' will become a FuncElement
 * while the predicate Function becomes another LambdaElement. But our QueryTreeVisitor can only handle MethodElement.
 * Thus we need to rewrite the 'filter' FuncElement into a MethodElement.
 */
private class PostMacroRewriter extends QueryTreeVisitor {
  import BinaryExpressionType.ITEM_IS_IN
  import PostMacroRewriter._
  import Query._

  override protected def handleBinaryExpression(binary: BinaryExpressionElement): RelationElement = {
    super.handleBinaryExpression(binary) match {
      case BinaryExpressionElement(ITEM_IS_IN, ConstValueElement(v, _), r, _) if r.rowTypeInfo <:< classOf[Option[_]] =>
        // x.optionField.contains(y) ==> x.optionField == Some(y)
        val newValue = AsyncValueHolder.map(v, Some(_))
        ElementFactory.equal(r, ElementFactory.constant(newValue, r.rowTypeInfo))
      case BinaryExpressionElement(ITEM_IS_IN, l, ConstValueElement(v: AsyncValueHolder[_], rType), _) =>
        // asyncCollection.contains(x.a)
        val newConst = ElementFactory.constant(v.evaluateSync, rType) // we have to do this to make reducer work
        ElementFactory.makeBinary(ITEM_IS_IN, l, newConst)
      case BinaryExpressionElement(ITEM_IS_IN, l, r, _) if r.rowTypeInfo == TypeInfo.STRING =>
        val desc = new RuntimeMethodDescriptor(r.rowTypeInfo, "contains", TypeInfo.BOOLEAN)
        visitElement(ElementFactory.call(r, desc, l :: Nil))
      case x => x
    }
  }

  /**
   * x.optionField.isDefined ==> x.optionField != None x.optionField.isEmpty ==> x.optionField == None
   */
  override protected def handleMemberRef(member: MemberElement): RelationElement = {
    super.handleMemberRef(member) match {
      case m: MemberElement if m.member.declaringType <:< classOf[Option[_]] =>
        m.member.name match {
          case "isDefined" | "nonEmpty" =>
            ElementFactory.notEqual(m.instanceProvider, ElementFactory.constant(None, m.member.declaringType))
          case "isEmpty" =>
            ElementFactory.equal(m.instanceProvider, ElementFactory.constant(None, m.member.declaringType))
          case _ =>
            m
        }
      case m: MemberElement
          if m.member.declaringType <:< classOf[Iterable[_]] && !RelationalUtils.isImmutableArrayOfByte(
            m.member.declaringType) =>
        m.member.name match {
          case "isEmpty" | "nonEmpty" | "size" =>
            val method = new RuntimeMethodDescriptor(m.member.declaringType, m.member.name, m.rowTypeInfo)
            ElementFactory.call(m.instanceProvider, method, Nil)
          case _ =>
            m
        }
      case m: MemberElement if m.member.declaringType <:< classOf[Knowable[_]] =>
        m.member.name match {
          case "toOption" | "isEmpty" | "nonEmpty" | "isDefined" =>
            val method = new RuntimeMethodDescriptor(m.member.declaringType, m.member.name, m.rowTypeInfo)
            ElementFactory.call(m.instanceProvider, method, Nil)
          case _ =>
            m
        }
      case m: MemberElement if convertFunctions.contains(m.member.name) && isPrimitive(m.rowTypeInfo) =>
        if (isPrimitive(m.member.declaringType)) {
          ElementFactory.convert(m.instanceProvider, m.rowTypeInfo)
        } else if (m.member.declaringType <:< classOf[Number]) {
          val methodName = s"${m.rowTypeInfo.runtimeClassName}Value"
          val method = new RuntimeMethodDescriptor(m.member.declaringType, methodName, m.rowTypeInfo)
          ElementFactory.call(m.instanceProvider, method, Nil)
        } else m
      case x => x
    }
  }

  private def isPrimitive(t: TypeInfo[_]): Boolean = {
    t.classes.size == 1 && t.clazz.isPrimitive
  }

  override protected def handleConstValue(const: ConstValueElement): RelationElement = {
    const.value match {
      case q: Query[_] => q.element
      case _           => const
    }
  }

  override protected def handleFuncCall(func: FuncElement): RelationElement = {
    func match {
      case FuncElement(c: MethodCallee, _, _) if c.method.declaringType <:< classOf[Query[_]] =>
        val instance = visitElement(func.instance)
        val arguments = visitElementList(func.arguments)
        c.name match {
          case "map"            => rewriteMap(instance, arguments)
          case "withFilter"     => rewriteFilter(instance, arguments)
          case "filter"         => rewriteFilter(instance, arguments)
          case "flatMap"        => rewriteFlatMap(instance, arguments)
          case "sortBy"         => rewriteSortBy(instance, arguments)
          case "groupBy"        => rewriteGroupBy(instance, arguments)
          case "mapValues"      => rewriteMapValues(instance, arguments)
          case "distinct"       => rewriteDistinct(instance, arguments)
          case "take"           => rewriteTake(instance, arguments)
          case "takeFirst"      => rewriteTake(instance, arguments, 0)
          case "takeRight"      => rewriteTake(instance, arguments, -1)
          case "innerJoin"      => rewriteJoin(INNER_JOIN, instance, arguments)
          case "leftOuterJoin"  => rewriteJoin(LEFT_OUTER_JOIN, instance, arguments)
          case "rightOuterJoin" => rewriteJoin(RIGHT_OUTER_JOIN, instance, arguments)
          case "fullOuterJoin"  => rewriteJoin(FULL_OUTER_JOIN, instance, arguments)
          case "on"             => rewriteOn(instance, arguments)
          case "sum" | "max" | "min" | "average" | "count" | "variance" | "stddev" | "corr" =>
            rewriteAggregate(c, instance, arguments)
          case _ => updateFuncCall(func, instance, arguments)
        }

      case FuncElement(c: MethodCallee, _, _)
          if c.method.declaringType <:< classOf[ExtendedEqualOps[_]] && c.method.name == "$eq$tilde$eq" =>
        val instance = visitElement(func.instance)
        val arguments = visitElementList(func.arguments)
        val transformedInstOpt = instance match {
          case FuncElement(c: MethodCallee, args, _) if c.method.name == "ExtendedEqualOps" => Some(args.head)
          case _                                                                            => None
        }
        transformedInstOpt.flatMap { inst =>
          arguments match {
            case ExpressionListElement(ConstValueElement(v1, _) :: Nil) :: ExpressionListElement(
                  ConstValueElement(conv: PriqlConverter[Any, Any] @unchecked, _) :: Nil) :: Nil =>
              val const = v1 match {
                case a: AsyncValueHolder[_] =>
                  AsyncValueHolder.constant(() => conv.convert(a.evaluate), inst.rowTypeInfo)
                case v => AsyncValueHolder.constant(() => conv.convert(v), inst.rowTypeInfo)
              }
              Some(ElementFactory.equal(inst, const))
            case _ => None
          }
        } getOrElse {
          updateFuncCall(func, instance, arguments)
        }
      case _ => super.handleFuncCall(func)
    }
  }

  private def rewriteOn(src: RelationElement, arguments: List[RelationElement]): RelationElement = {
    arguments match {
      case List(l: LambdaElement) =>
        src match {
          case MethodElement(mc, args, srcType, srcKey, pos) =>
            val newMethodArgs = args ::: List(MethodArg[RelationElement]("on", l))
            new MethodElement(mc, newMethodArgs, srcType, srcKey, pos)
          case _ =>
            throw new RelationalUnsupportedException(s"Unsupported case: 'on' must be adjacent to join method.")
        }
      case _ =>
        throw new RelationalException(s"Illegal arguments: $arguments")
    }
  }

  private def rewriteJoin(
      joinType: QueryMethod,
      src: RelationElement,
      arguments: List[RelationElement]): RelationElement = {
    arguments match {
      case ExpressionListElement(List(right)) :: _ =>
        val leftType = findShapeType(src)
        val rightType = findShapeType(right)
        val shapeType = TupleTypeInfo(leftType, rightType)
        new MethodElement(joinType, List(MethodArg("left", src), MethodArg("right", right)), shapeType, NoKey)
      case _ =>
        throw new RelationalException(s"Illegal arguments: $arguments")
    }
  }

  private def rewriteMap(src: RelationElement, arguments: List[RelationElement]): RelationElement = {
    arguments match {
      case ExpressionListElement(List(l)) :: ExpressionListElement(
            ConstValueElement(resultType: TypeInfo[_], _) :: _) :: Nil =>
        new MethodElement(OUTPUT, List(MethodArg("src", src), MethodArg("f", l)), resultType, NoKey)
      case _ =>
        throw new RelationalException(s"Illegal arguments: $arguments")
    }
  }

  private def rewriteFilter(src: RelationElement, arguments: List[RelationElement]): RelationElement = {
    arguments match {
      case ExpressionListElement(List(l)) :: _ =>
        new MethodElement(WHERE, List(MethodArg("src", src), MethodArg("p", l)), findShapeType(src), NoKey)
      case _ =>
        throw new RelationalException(s"Illegal arguments: $arguments")
    }
  }

  private def rewriteFlatMap(src: RelationElement, arguments: List[RelationElement]): RelationElement = {
    arguments match {
      // flatMap has three implicit parameters: QueryConverter, TypeInfo and Position
      case List(
            ExpressionListElement(List(l)),
            ExpressionListElement(conv :: ConstValueElement(resultType: TypeInfo[_], _) :: _)) =>
        new MethodElement(
          FLATMAP,
          List(MethodArg("src", src), MethodArg("f", l), MethodArg("conv", conv)),
          resultType,
          NoKey)
      case _ =>
        throw new RelationalException(s"Illegal arguments: $arguments")
    }
  }

  private def rewriteSortBy(src: RelationElement, arguments: List[RelationElement]): RelationElement = {
    arguments match {
      case ExpressionListElement(List(l)) :: ExpressionListElement(ord :: _) :: Nil =>
        new MethodElement(
          SORT,
          List(MethodArg("src", src), MethodArg("f", l), MethodArg("ordering", ord)),
          findShapeType(src),
          NoKey)
      case _ =>
        throw new RelationalException(s"Illegal arguments: $arguments")
    }
  }

  private def rewriteGroupBy(src: RelationElement, arguments: List[RelationElement]): RelationElement = {
    arguments match {
      case ExpressionListElement(List(l)) :: ExpressionListElement(
            ConstValueElement(keyType: TypeInfo[_], _) :: _) :: Nil =>
        val valueType = TypeInfo(classOf[Query[_]], findShapeType(src))
        val shapeType = TupleTypeInfo(keyType, valueType)
        new MethodElement(
          GROUP_BY,
          List(MethodArg("src", src), MethodArg("f", l), MethodArg("pvd", new ConstValueElement(QueryProvider.NoKey))),
          shapeType,
          NoKey)
      case _ =>
        throw new RelationalException(s"Illegal arguments: $arguments")
    }
  }

  private def rewriteMapValues(src: RelationElement, arguments: List[RelationElement]): RelationElement = {
    arguments match {
      case ExpressionListElement(List(l)) :: ExpressionListElement(
            ConstValueElement(resultType: TypeInfo[_], _) :: _) :: Nil =>
        new MethodElement(GROUP_MAP_VALUES, List(MethodArg("src", src), MethodArg("f", l)), resultType, NoKey)
      case _ =>
        throw new RelationalException(s"Illegal arguments: $arguments")
    }
  }

  private def rewriteAggregate(
      c: MethodCallee,
      src: RelationElement,
      arguments: List[RelationElement]): RelationElement = {
    arguments match {
      case ExpressionListElement(l1) :: ExpressionListElement(l2 @ List(ag)) :: _ =>
        ElementFactory.call(src, c.method, l1 ::: l2)
      case x @ (_ :: _ :: _) =>
        throw new MatchError(x)
      case x :: Nil =>
        if (c.name == "count")
          ElementFactory.call(src, c.method, List(x, ElementFactory.constant(Count.Boolean)))
        else
          ElementFactory.call(src, c.method, List(x))
      case Nil =>
        ElementFactory.call(src, c.method, List(ElementFactory.constant(Count.Any)))
    }
  }

  private def rewriteDistinct(src: RelationElement, arguments: List[RelationElement]): RelationElement = {
    new MethodElement(TAKE_DISTINCT, List(MethodArg("src", src)), findShapeType(src), NoKey)
  }

  private def rewriteTake(
      src: RelationElement,
      arguments: List[RelationElement],
      intOffset: Int = 0): RelationElement = {
    arguments match {
      case ExpressionListElement(List(offset, numRows)) :: _ =>
        new MethodElement(
          TAKE,
          List(MethodArg("src", src), MethodArg("offset", offset), MethodArg("numRows", numRows)),
          findShapeType(src),
          NoKey)
      case ExpressionListElement(List(numRows)) :: _ =>
        val offset = ElementFactory.constant(intOffset, TypeInfo.INT)
        new MethodElement(
          TAKE,
          List(MethodArg("src", src), MethodArg("offset", offset), MethodArg("numRows", numRows)),
          findShapeType(src),
          NoKey)
      case _ =>
        throw new RelationalException(s"Illegal arguments: $arguments")
    }
  }

}

object PostMacroRewriter {
  def rewrite(l: LambdaElement): LambdaElement = {
    new PostMacroRewriter().visitElement(l).asInstanceOf[LambdaElement]
  }

  val convertFunctions = Set("toDouble", "toInt", "toLong", "toFloat", "toShort", "toByte")
}
