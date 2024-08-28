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

import optimus.platform.relational.RelationalUnsupportedException
import scala.collection.mutable.ListBuffer

class QueryTreeVisitor {

  def visitElement(element: RelationElement): RelationElement = {
    if (element eq null) null
    else
      element match {
        case e: MethodElement              => handleMethod(e)
        case e: UnaryExpressionElement     => handleUnaryExpression(e)
        case e: BinaryExpressionElement    => handleBinaryExpression(e)
        case e: ParameterElement           => handleParameter(e)
        case e: MemberElement              => handleMemberRef(e)
        case e: ConstValueElement          => handleConstValue(e)
        case e: FuncElement                => handleFuncCall(e)
        case e: ExpressionListElement      => handleExpressionList(e)
        case e: ProviderRelation           => handleQuerySrc(e)
        case e: NewElement                 => handleNew(e)
        case e: LambdaElement              => handleLambda(e)
        case e: AggregateExpressionElement => handleAggregateExpression(e)
        case e: ConditionalElement         => handleConditional(e)
        case e: TypeIsElement              => handleTypeIs(e)
        case _ => throw new RelationalUnsupportedException(s"Unsupported elementType: ${element.elementType}")
      }
  }

  protected def handleTypeIs(typeIs: TypeIsElement): RelationElement = {
    val e = visitElement(typeIs.element)
    updateTypeIs(typeIs, e)
  }

  final protected def updateTypeIs(typeIs: TypeIsElement, element: RelationElement): RelationElement = {
    if (typeIs.element eq element) typeIs
    else new TypeIsElement(element, typeIs.targetType)
  }

  protected def handleConditional(c: ConditionalElement): RelationElement = {
    val test = visitElement(c.test)
    val ifTrue = visitElement(c.ifTrue)
    val ifFalse = visitElement(c.ifFalse)
    updateConditional(c, test, ifTrue, ifFalse)
  }

  final protected def updateConditional(
      c: ConditionalElement,
      test: RelationElement,
      ifTrue: RelationElement,
      ifFalse: RelationElement): ConditionalElement = {
    if ((test ne c.test) || (ifTrue ne c.ifTrue) || (ifFalse ne c.ifFalse))
      ElementFactory.condition(test, ifTrue, ifFalse, c.projectedType())
    else c
  }

  protected def handleMethod(method: MethodElement): RelationElement = {
    val newArgs = visitArgList(method.methodArgs)
    if (newArgs ne method.methodArgs) method.replaceArgs(newArgs) else method
  }

  protected def handleQuerySrc(element: ProviderRelation): RelationElement = element

  protected def handleExpressionList(element: ExpressionListElement): RelationElement = {

    val tmpList = this.visitElementList(element.exprList)
    if (tmpList eq element.exprList)
      element
    else
      new ExpressionListElement(tmpList, element.rowTypeInfo)
  }

  protected def handleUnaryExpression(unary: UnaryExpressionElement): RelationElement = {
    val operand = this.visitElement(unary.element)
    updateUnary(unary, operand)
  }

  final protected def updateUnary(unary: UnaryExpressionElement, operand: RelationElement): RelationElement = {
    if (operand eq unary.element) unary else new UnaryExpressionElement(unary.op, operand, unary.rowTypeInfo)
  }

  protected def handleBinaryExpression(element: BinaryExpressionElement): RelationElement = {
    val left = this.visitElement(element.left)
    val right = this.visitElement(element.right)
    updateBinary(element, left, right)
  }

  final protected def updateBinary(
      b: BinaryExpressionElement,
      left: RelationElement,
      right: RelationElement): BinaryExpressionElement = {
    if ((left ne b.left) || (right ne b.right)) {
      if (b.method ne null) new MethodBinaryExpressionElement(b.op, left, right, b.method)
      else new BinaryExpressionElement(b.op, left, right, b.rowTypeInfo)
    } else b
  }

  protected def handleParameter(element: ParameterElement): RelationElement = element

  protected def handleMemberRef(element: MemberElement): RelationElement = {
    val instanceProvider = this.visitElement(element.instanceProvider)
    updateMemberRef(element, instanceProvider)
  }

  final protected def updateMemberRef(element: MemberElement, instanceProvider: RelationElement): MemberElement = {
    if (instanceProvider eq element.instanceProvider)
      element
    else ElementFactory.makeMemberAccess(instanceProvider, element.member)
  }

  protected def handleConstValue(element: ConstValueElement): RelationElement = element

  protected def handleFuncCall(func: FuncElement): RelationElement = {
    val instance = visitElement(func.instance)
    val arguments = visitElementList(func.arguments)
    updateFuncCall(func, instance, arguments)
  }

  final protected def updateFuncCall(
      func: FuncElement,
      instance: RelationElement,
      arguments: List[RelationElement]): FuncElement = {
    if ((instance ne func.instance) || (arguments ne func.arguments))
      new FuncElement(func.callee, arguments, instance)
    else
      func
  }

  protected def handleNew(element: NewElement): RelationElement = {
    val arguments = this.visitElementList(element.arguments)
    if (arguments.eq(element.arguments))
      element
    else
      ElementFactory.makeNew(element.ctor, arguments, element.members)
  }

  protected def handleLambda(lambda: LambdaElement): RelationElement = {
    val body = visitElement(lambda.body)
    if (body eq lambda.body) lambda
    else {
      ElementFactory.lambda(body, lambda.parameters)
    }
  }

  protected def handleAggregateExpression(element: AggregateExpressionElement): RelationElement = element

  /**
   * Visit all Method Args to replace existing MethodArgs.
   */
  final protected def visitArgList(srcList: List[MethodArg[RelationElement]]): List[MethodArg[RelationElement]] = {
    val tmpList = new ListBuffer[MethodArg[RelationElement]]
    var replace = false
    for (i <- srcList) {
      val res = visitElement(i.param)
      replace = replace || (res ne i.param)
      tmpList += new MethodArg[RelationElement](i.name, res)
    }
    if (replace) tmpList.result() else srcList
  }

  final protected def visitElementList(list: List[RelationElement]): List[RelationElement] = {
    if (list eq null) null
    else {
      var count = 0;
      var listBuffer: ListBuffer[RelationElement] = null
      for (e <- list) {
        val elem = this.visitElement(e)
        if (listBuffer != null)
          listBuffer += elem
        else if (elem.ne(e)) {
          listBuffer = new ListBuffer[RelationElement]()
          listBuffer ++= list.take(count)
          listBuffer += elem
        }
        count += 1
      }
      if (listBuffer != null) listBuffer.result() else list
    }
  }
}
