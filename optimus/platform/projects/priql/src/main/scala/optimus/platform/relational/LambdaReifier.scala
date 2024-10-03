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

import optimus.platform.GroupQuery
import optimus.platform.JoinQuery
import optimus.platform.Query
import optimus.platform.relational.tree.BinaryExpressionType.BinaryExpressionType
import optimus.platform.relational.tree._

import scala.reflect.macros.blackbox.Context

class LambdaReifier[C <: Context](val c: C) extends AbstractLambdaReifier {
  import c.universe._

  private val ElementFactoryExT = Ident(typeOf[ElementFactoryEx.type].termSymbol)

  override protected def transformConstant(constant: Tree): Tree = {
    val AsyncValueHolderT = Ident(typeOf[AsyncValueHolder.type].termSymbol)
    q"$AsyncValueHolderT.constant(() => ${c.untypecheck(constant)}, ${Helper.mkTypeInfo(constant.tpe)})"
  }

  override protected def transformBinary(opEncoded: String, left: Tree, right: Tree, method: Tree): Tree = {
    import Helper._
    opEncoded match {
      case "$bar$bar" =>
        q"$ElementFactoryExT.orElse($left, $right)"
      case "$amp$amp" =>
        q"$ElementFactoryExT.andAlso($left, $right)"
      case _ =>
        super.transformBinary(opEncoded, left, right, method)
    }
  }

  override protected def canTranslateToMethodElement(s: Symbol): Boolean = {
    import Constants._
    val owner = s.owner
    (owner eq QuerySym) || (owner eq GroupQuerySym) || (owner eq JoinQuerySym)
  }

  override protected def checkMethod(s: Symbol, encodedName: String): Unit = {
    if (canTranslateToMethodElement(s)) {
      if (Constants.unsupportedQueryOps.contains(encodedName))
        throw new RelationalUnsupportedException(s"Unsupported Query operator: $encodedName")
      if (encodedName == "groupBy" && !s.isMacro)
        throw new RelationalUnsupportedException(s"Unsupported Query operator: groupByTyped")
      if (encodedName == "on" && !s.isMacro)
        throw new RelationalUnsupportedException(s"Unsupported Query operator: onList")
    }
  }

  protected object Constants {
    val QuerySym = typeOf[Query[_]].typeSymbol
    val GroupQuerySym = typeOf[GroupQuery[_, _]].typeSymbol
    val JoinQuerySym = typeOf[JoinQuery[_, _]].typeSymbol

    val unsupportedQueryOps = Set(
      "aggregateBy",
      "aggregateByImplicit",
      "aggregateByUntyped",
      "union",
      "merge",
      "difference",
      "untype",
      "extend",
      "shapeTo",
      "extendTyped",
      "extendTypedValue",
      "replace",
      "replaceValue",
      "arrange",
      "aggregate",
      "foldLeft",
      "pivotOn",
      "pivotOnTyped",
      "withLeftDefault",
      "withRightDefault",
      "onNatural"
    )
  }
}

object ElementFactoryEx {
  def andAlso(left: RelationElement, right: => RelationElement): RelationElement = {
    left match {
      case ConstValueElement(v: Boolean, _) =>
        if (!v) left else right
      case ConstValueElement(a: AsyncValueHolder[Boolean @unchecked], _) =>
        if (!a.evaluateSync) ElementFactory.constant(false, TypeInfo.BOOLEAN) else right
      case _ =>
        right match {
          case r @ ConstValueElement(v: Boolean, _) =>
            if (!v) r else left
          case ConstValueElement(a: AsyncValueHolder[Boolean @unchecked], _) =>
            if (!a.evaluateSync) ElementFactory.constant(false, TypeInfo.BOOLEAN) else left
          case r => ElementFactory.andAlso(left, r)
        }
    }
  }

  def orElse(left: RelationElement, right: => RelationElement): RelationElement = {
    left match {
      case ConstValueElement(v: Boolean, _) =>
        if (v) left else right
      case ConstValueElement(a: AsyncValueHolder[Boolean @unchecked], _) =>
        if (a.evaluateSync) ElementFactory.constant(true, TypeInfo.BOOLEAN) else right
      case _ =>
        right match {
          case r @ ConstValueElement(v: Boolean, _) =>
            if (v) r else left
          case ConstValueElement(a: AsyncValueHolder[Boolean @unchecked], _) =>
            if (a.evaluateSync) ElementFactory.constant(true, TypeInfo.BOOLEAN) else left
          case r => ElementFactory.orElse(left, r)
        }
    }
  }
}
