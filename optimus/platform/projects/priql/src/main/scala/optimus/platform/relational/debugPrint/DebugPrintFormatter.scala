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
package optimus.platform.relational.debugPrint

import optimus.graph.DiagnosticSettings
import optimus.platform.relational.namespace.NamespaceMultiRelation
import optimus.platform.relational.tree.BinaryExpressionElement
import optimus.platform.relational.tree.BinaryExpressionType
import optimus.platform.relational.tree.ConstValueElement
import optimus.platform.relational.tree.ElementType.MemberRef
import optimus.platform.relational.tree.ExpressionListElement
import optimus.platform.relational.tree.FuncElement
import optimus.platform.relational.tree.MemberElement
import optimus.platform.relational.tree.MethodArg
import optimus.platform.relational.tree.MethodCallee
import optimus.platform.relational.tree.MethodElement
import optimus.platform.relational.tree.ParameterElement
import optimus.platform.relational.tree.ProviderRelation
import optimus.platform.relational.tree.QueryMethod
import optimus.platform.relational.tree.QueryTreeVisitor
import optimus.platform.relational.tree.RelationElement
import optimus.platform.relational.tree.TypeInfo

private class DebugPrintFormatter(sb: StringBuilder, listAbbreviationThreshold: Int) extends QueryTreeVisitor {
  import DebugPrintFormatter._

  private[this] var inContainsList = false

  protected override def handleFuncCall(func: FuncElement): RelationElement = {
    func match {
      case FuncElement(mc: MethodCallee, List(c @ ConstValueElement(_: String, _)), m: MemberElement)
          if mc.method.declaringType == TypeInfo.STRING =>
        visitElement(m)
        sb.append(".")
        sb.append(mc.name)
        sb.append("(")
        visitElement(c)
        sb.append(")")
      case FuncElement(
            mc: MethodCallee,
            List(c @ ConstValueElement(_: String, _)),
            FuncElement(_, List(m: MemberElement), _))
          if mc.method.declaringType.runtimeClassName == "optimus.security.package$OptionStringOps" =>
        visitElement(m)
        sb.append(".")
        sb.append(mc.name)
        sb.append("(")
        visitElement(c)
        sb.append(")")
      case _ =>
        super.handleFuncCall(func)
    }
    func
  }

  protected override def handleQuerySrc(element: ProviderRelation): RelationElement = {
    sb.append("from(")
    element match {
      case e: NamespaceMultiRelation[_] =>
        sb.append("Namespace(\"")
          .append(e.source.namespace)
          .append("\", includesSubPackage=")
          .append(e.source.includesSubPackage)
          .append(")")
      case _ => sb.append(element.rowTypeInfo.name)
    }
    sb.append(")")
    element
  }

  protected override def handleMethod(method: MethodElement): RelationElement = {
    import QueryMethod._
    method.methodCode match {
      case WHERE   => handleWhere(method)
      case EXTRACT => handleExtract(method)
      case OUTPUT  => handleOutput(method)
      case _       => throw new UnsupportedOperationException(s"Unsupported server side method: ${method.methodCode}")
    }
  }

  private def handleOutput(method: MethodElement): RelationElement = {
    val (s :: others) = method.methodArgs
    visitElement(s.param)
    sb.append(".map(t => ")
    others match {
      case MethodArg(_, ExpressionListElement((_: FuncElement) :: Nil)) :: Nil => sb.append("t")
      case _                                                                   => visitArgList(others)
    }
    sb.append(")")
    method
  }

  private def handleWhere(method: MethodElement): RelationElement = {
    val (s :: others) = method.methodArgs
    visitElement(s.param)
    sb.append(".filter(t => ")
    visitArgList(others)
    sb.append(")")
    method
  }

  private def handleExtract(method: MethodElement): RelationElement = {
    val (s :: others) = method.methodArgs
    visitElement(s.param)

    val ExpressionListElement(ConstValueElement(isAsc: Boolean, _) :: ConstValueElement(count: Int, _) :: Nil) =
      others.head.arg
    sb.append(".extractByTT")
    if (!isAsc) sb.append("Desc")
    sb.append("(")
    sb.append(count)
    sb.append(")")
    method
  }

  protected override def handleConstValue(element: ConstValueElement): RelationElement = {
    element.value match {
      case s: String =>
        sb.append('"')
        sb.append(s)
        sb.append('"')
      case s: Seq[_] if (element.projectedType() ne null) && element.projectedType().clazz == classOf[Option[_]] =>
        sb.append(s.find(t => true))
      case value => sb.append(value)
    }
    element
  }

  protected override def handleBinaryExpression(element: BinaryExpressionElement): RelationElement = {
    import BinaryExpressionType._
    def visitBinaryExpression(
        operator: String,
        left: RelationElement = element.left,
        right: RelationElement = element.right) = {
      visitElement(left)
      sb.append(operator)
      visitElement(right)
    }
    element.op match {
      case ITEM_IS_IN =>
        sb.append("Seq(")
        inContainsList = true
        visitElement(ConstTypeInfoBinder.bind(element.right, element.left.projectedType()))
        inContainsList = false
        sb.append(").contains(")
        visitElement(element.left)
        sb.append(")")

      case EQ =>
        visitBinaryExpression(" == ", right = ConstTypeInfoBinder.bind(element.right, element.left.projectedType()))

      case BOOLAND => visitBinaryExpression(" && ")

      case NE =>
        visitBinaryExpression(" != ", right = ConstTypeInfoBinder.bind(element.right, element.left.projectedType()))

      case BOOLOR => visitBinaryExpression(" || ")

      case _ =>
        throw new UnsupportedOperationException(s"Unsupported server side binary expression type: ${element.op}")
    }
    element
  }

  protected override def handleMemberRef(element: MemberElement): RelationElement = {
    element match {
      case MemberElement(p: ParameterElement, member) => sb.append(s"t.$member")
      case e if e.elementType == MemberRef =>
        visitElement(e.instanceProvider)
        sb.append(".").append(e.memberName)
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported server side member instance type: ${element.instanceProvider.elementType}")
    }
    element
  }

  protected override def handleExpressionList(element: ExpressionListElement): RelationElement = {
    if (inContainsList) {
      val list = element.exprList
      val size = list.size
      if (size > math.max(2, math.min(MaxThreshold, listAbbreviationThreshold))) {
        visitElement(list.head)
        sb.append(s"... [${size - 2} elements]... ")
        visitElement(list(size - 1))
        element
      } else {
        var first = true
        for (e <- element.exprList) {
          if (first) first = false else sb.append(", ")
          visitElement(e)
        }
        element
      }
    } else super.handleExpressionList(element)
  }

  /**
   * To correct the typeInfo after deserialization, e.g. Option[_] becomes List[_] at server side
   */
  private class ConstTypeInfoBinder(t: TypeInfo[_]) extends QueryTreeVisitor {
    protected override def handleConstValue(element: ConstValueElement): RelationElement = {
      new ConstValueElement(element.value, t)
    }
  }

  private object ConstTypeInfoBinder {
    def bind(e: RelationElement, t: TypeInfo[_]): RelationElement = {
      new ConstTypeInfoBinder(t).visitElement(e)
    }
  }
}

object DebugPrintFormatter {
  val MaxThreshold: Int = Int.MaxValue - 1
  lazy val DefaultThreshold: Int =
    DiagnosticSettings.getIntProperty("optimus.priql.print.expressionListAbbreviationThreshold", 10)
  private def format(e: RelationElement, sb: StringBuilder, listAbbreviationThreshold: Int): Unit = {
    new DebugPrintFormatter(sb, listAbbreviationThreshold).visitElement(e)
  }

  def format(e: RelationElement, threshold: Int = DefaultThreshold): String = {
    val sb = new StringBuilder()
    DebugPrintFormatter.format(e, sb, threshold)
    sb.toString()
  }
}
