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
package optimus.platform.relational.execution

import optimus.platform.relational.tree._
import optimus.platform.relational.internal._
import optimus.platform.relational._
import optimus.platform.DynamicObject

abstract class LambdaExecNode(typeInfo: TypeInfo[_]) extends IExecNode(typeInfo) {
  def evaluate(arg: Any): AnyRef

  // return one column of srcColumns without filtering according to validated indices
  def evaluateColumns(srcColumns: RelationColumnView): Any

}

object LambdaExecNode {

  /**
   * argType: the argument's type for lambda
   */
  def apply(element: BinaryExpressionElement, argType: TypeInfo[_]): LambdaExecNode = {
    if (argType == null || argType.clazz == null) throw new IllegalArgumentException("Do not have arg type")
    new BinaryExpressionLambdaExecNode(element, argType)
  }

  /**
   * argType: the argument's type for lambda
   */
  def apply(element: MemberElement, argType: TypeInfo[_]): LambdaExecNode = {
    new MemberLambdaExecNode[MemberElement](element, argType)
  }
}

private[optimus] class BinaryExpressionLambdaExecNode(val binExpr: BinaryExpressionElement, argType: TypeInfo[_])
    extends LambdaExecNode(binExpr.projectedType()) {
  def evaluate(arg: Any): AnyRef = {
    Boolean.box(ExpressionElementHelper.executeExpression(binExpr, arg).asInstanceOf[Boolean])
  }

  /**
   * call RelationColumnView to do binary calculation
   * @return
   *   #1 RelationColumn[_] #2 Boolean
   */
  def evaluateColumns(srcColumns: RelationColumnView): Any = {
    RelationColumnView.executeBinaryExpression(srcColumns, binExpr)
  }
}

private[optimus] class MemberLambdaExecNode[T <: MemberElement](val member: T, argType: TypeInfo[_])
    extends LambdaExecNode(member.projectedType()) {

  def evaluate(arg: Any): AnyRef = {

    val memberElement = member.instanceProvider match {
      case provider: MemberElement => new MemberLambdaExecNode(provider, null).evaluate(arg)
      case _                       => arg
    }

    memberElement match {
      case dynObj: DynamicObject => RelationalUtils.box(dynObj.get(member.memberName))
      case _ if argType >:> memberElement.getClass =>
        RelationalUtils.box(argType.propertyValue(member.memberName, memberElement))
      case _ => ReflectPropertyUtils.getPropertyValue(member.memberName, memberElement.getClass, memberElement)
    }
  }

  /**
   * fetch one of the columns according to member name, now we only allow one level access which is A.a cannot be
   * A.B.a..., but allow t._1._2.t1
   * @return
   *   RelationColumn[_]
   */
  def evaluateColumns(srcColumns: RelationColumnView): Any =
    srcColumns.src.columns.find(_.columnName == getName(member)).get

  private def getName(m: T): String =
    if (m.instanceProvider == null || m.instanceProvider.isInstanceOf[ParameterElement]) m.memberName
    else getName(m.instanceProvider.asInstanceOf[T]) + "." + m.memberName
}
