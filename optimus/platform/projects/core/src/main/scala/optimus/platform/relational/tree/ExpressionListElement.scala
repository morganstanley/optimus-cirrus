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

import ElementType._

/**
 * defines expression list element which can be used in a place where multiple expressions are needed e.g. select clause
 * or a like
 */
class ExpressionListElement(val exprList: List[RelationElement], typeInfo: TypeInfo[_])
    extends RelationElement(ExpressionList, typeInfo) {

  def this(exprs: List[RelationElement]) = this(exprs, null)

  override def prettyPrint(indent: Int, out: StringBuilder): Unit = {
    indentAndStar(indent, out)
    out ++= serial + " Expression List:\n"
    exprList.foreach(el => el.prettyPrint(indent + 1, out))
  }

  lazy val constValues: Set[Any] = exprList.iterator.collect { case x: ConstValueElement => x.value }.toSet

  override def toString = exprList.mkString("ExpressionListElement[", ", ", "]")
}

object ExpressionListElement {
  def unapply(element: ExpressionListElement): Some[List[RelationElement]] = Some(element.exprList)
}
object ExpressionListElementUnwrap {
  def unapplySeq(element: RelationElement): Some[List[RelationElement]] = element match {
    case el: ExpressionListElement => Some(el.exprList)
    case el                        => Some(el :: Nil)
  }
}

object FuncExpressionListElement {
  def unapply(element: ExpressionListElement) = {

    def process(
        elements: List[RelationElement],
        funcs: List[FuncElement] = Nil,
        exprs: List[ExpressionElement] = Nil,
        lambdas: List[LambdaElement] = Nil,
        exprLists: List[ExpressionListElement] = Nil): Option[(
        Option[List[FuncElement]],
        Option[List[ExpressionElement]],
        Option[List[LambdaElement]],
        Option[List[ExpressionListElement]])] = elements match {
      case Nil =>
        Some(
          funcs match {
            case Nil => None
            case _   => Some(funcs)
          },
          exprs match {
            case Nil => None
            case _   => Some(exprs)
          },
          lambdas match {
            case Nil => None
            case _   => Some(lambdas)
          },
          exprLists match {
            case Nil => None
            case _   => Some(exprLists)
          }
        )
      case (func: FuncElement) :: elementsTail       => process(elementsTail, func :: funcs, exprs, lambdas, exprLists)
      case (expr: ExpressionElement) :: elementsTail => process(elementsTail, funcs, expr :: exprs, lambdas, exprLists)
      case (lambda: LambdaElement) :: elementsTail => process(elementsTail, funcs, exprs, lambda :: lambdas, exprLists)
      case (exprList: ExpressionListElement) :: elementsTail =>
        process(elementsTail, funcs, exprs, lambdas, exprList :: exprLists)
      case _ => None
    }

    process(element.exprList.reverse)
  }
}
