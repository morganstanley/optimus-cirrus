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
package optimus.platform.relational.asm

import optimus.platform.asNode
import optimus.platform.relational.tree._

import scala.collection.mutable

object NodeAnalyzer {
  def analyze(e: RelationElement): (RelationElement, NodeDependency) = {
    new NodeAnalyzer().analyze(e)
  }
}

sealed class NodeAnalyzer private () {
  import Typer._

  private var visitedNodes: List[RelationElement] = Nil
  private val nodeDependencies = new mutable.HashMap[RelationElement, List[RelationElement]]
  private val nodes = new mutable.HashSet[RelationElement]
  private val inplaceAppliedAsyncLambda = new mutable.HashSet[FuncElement]

  def analyze(e: RelationElement): (RelationElement, NodeDependency) = {
    var elem = e
    analyzeElement(elem)
    while (inplaceAppliedAsyncLambda.nonEmpty) {
      elem = InplaceAsyncLambdaRewriter.rewrite(elem, inplaceAppliedAsyncLambda)
      visitedNodes = Nil
      nodeDependencies.clear()
      nodes.clear()
      inplaceAppliedAsyncLambda.clear()
      analyzeElement(elem)
    }
    (elem, NodeDependency(nodes.toSet, nodeDependencies.toMap))
  }

  private def analyzeElement(e: RelationElement): Unit = {
    import BinaryExpressionType._
    import UnaryExpressionType._

    e match {
      case null                 =>
      case c: ConstValueElement =>
      case p: ParameterElement  =>
      case m: MemberElement     => analyzeMember(m)
      case f: FuncElement       => analyzeFunc(f)
      case b: BinaryExpressionElement =>
        if (b.op == BOOLAND || b.op == BOOLOR)
          analyzeLogicalBinary(b)
        else analyzeBinary(b)
      case c: ConditionalElement                        => analyzeCondition(c)
      case l: LambdaElement                             => analyzeLambda(l)
      case c: UnaryExpressionElement if c.op == CONVERT => analyzeConvert(c)
      case n: NewElement                                => analyzeNew(n)
      case t: TypeIsElement                             => analyzeTypeIs(t)
      case _                                            => ???
    }
  }

  private def analyzeLogicalNode(e: RelationElement, analyzeChildren: => Unit): Unit = {
    nodes.add(e)
    val saved = visitedNodes
    visitedNodes = Nil
    analyzeChildren
    nodeDependencies.put(e, visitedNodes)
    visitedNodes = e :: saved
  }

  private def analyzeNew(n: NewElement): Unit = {
    n.arguments.foreach(analyzeElement)
  }

  private def analyzeFunc(f: FuncElement): Unit = {
    val mc = f.callee.asInstanceOf[MethodCallee]
    if (mc.method.metadata.contains(MetaMethodAsync)) {
      analyzeLogicalNode(
        f, {
          analyzeElement(f.instance)
          f.arguments.foreach(analyzeElement)
        })
    } else {
      analyzeElement(f.instance)
      if (
        (f.instance ne null) && f.instance.elementType == ElementType.Lambda
        && f.callee.name == "apply" && nodeDependencies(f.instance).nonEmpty
      )
        inplaceAppliedAsyncLambda.add(f)
      f.arguments.foreach(analyzeElement)
    }
  }

  private def analyzeConvert(c: UnaryExpressionElement): Unit = {
    analyzeElement(c.element)
  }

  private def analyzeTypeIs(t: TypeIsElement): Unit = {
    analyzeElement(t.element)
  }

  private def analyzeMember(m: MemberElement): Unit = {
    if (m.member.metadata.contains(MetaMethodAsync)) {
      analyzeLogicalNode(m, analyzeElement(m.instanceProvider))
    } else {
      analyzeElement(m.instanceProvider)
    }
  }

  private def analyzeCondition(c: ConditionalElement): Unit = {
    val saved = visitedNodes
    visitedNodes = Nil
    var isLogicalNode = false
    analyzeElement(c.ifTrue)
    if (visitedNodes.nonEmpty) {
      isLogicalNode = true
      if (!nodeDependencies.contains(c.ifTrue))
        nodeDependencies.put(c.ifTrue, visitedNodes)
      visitedNodes = Nil
    }
    analyzeElement(c.ifFalse)
    if (visitedNodes.nonEmpty) {
      isLogicalNode = true
      if (!nodeDependencies.contains(c.ifFalse))
        nodeDependencies.put(c.ifFalse, visitedNodes)
    }
    visitedNodes = saved
    if (isLogicalNode) {
      analyzeLogicalNode(c, analyzeElement(c.test))
    } else {
      analyzeElement(c.test)
    }
  }

  private def analyzeLogicalBinary(b: BinaryExpressionElement): Unit = {
    val saved = visitedNodes
    visitedNodes = Nil
    analyzeElement(b.right)

    if (visitedNodes.isEmpty) {
      // right side has no nodes
      visitedNodes = saved
      analyzeElement(b.left)
    } else {
      if (!nodeDependencies.contains(b.right))
        nodeDependencies.put(b.right, visitedNodes)
      visitedNodes = saved
      analyzeLogicalNode(b, analyzeElement(b.left))
    }
  }

  private def analyzeBinary(b: BinaryExpressionElement): Unit = {
    analyzeElement(b.left)
    analyzeElement(b.right)
  }

  private def analyzeLambda(l: LambdaElement): Unit = {
    val saved = visitedNodes
    visitedNodes = Nil
    analyzeElement(l.body)
    nodeDependencies.put(l, visitedNodes)
    visitedNodes = saved
  }
}

final case class NodeDependency(nodes: Set[RelationElement], dependencies: Map[RelationElement, List[RelationElement]])

private class InplaceAsyncLambdaRewriter(candidates: mutable.HashSet[FuncElement]) extends QueryTreeVisitor {
  protected override def handleFuncCall(func: FuncElement): RelationElement = {
    val inst = visitElement(func.instance)
    val args = visitElementList(func.arguments)
    if (candidates.contains(func)) {
      val mc = func.callee.asInstanceOf[MethodCallee]
      // 'inst' is a LambdaElement with async body, rewrite to "asNode.apply$withNode($inst).apply($args)"
      // with necessary Typer generated metadata
      val nodeFuncionType = TypeInfo.javaTypeInfo(Typer.nodeFunctionClass(args.size))
      val applyWithNode =
        new RuntimeMethodDescriptor(TypeInfo.javaTypeInfo(asNode.getClass), "apply$withNode", nodeFuncionType)
      val newInst = ElementFactory.call(null, applyWithNode, inst :: Nil)
      val apply = new RuntimeMethodDescriptor(nodeFuncionType, "apply", mc.method.returnType)
      Typer.typeCheck(ElementFactory.call(newInst, apply, args))
    } else updateFuncCall(func, inst, args)
  }
}

private object InplaceAsyncLambdaRewriter {
  def rewrite(e: RelationElement, candidates: mutable.HashSet[FuncElement]): RelationElement = {
    new InplaceAsyncLambdaRewriter(candidates).visitElement(e)
  }
}
