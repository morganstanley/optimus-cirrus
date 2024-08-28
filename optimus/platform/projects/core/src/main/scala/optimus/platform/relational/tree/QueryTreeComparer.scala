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

class QueryTreeComparer protected (
    private var paramScope: ScopedHashMap[ParameterElement, ParameterElement],
    fnCompare: (Any, Any) => Boolean) {

  protected def compare(a: RelationElement, b: RelationElement): Boolean = {
    if (a eq b) true
    else if (a == null || b == null) false
    else if (a.elementType != b.elementType) false
    else if (a.rowTypeInfo != b.rowTypeInfo) false
    else
      (a, b) match {
        case (ma: MethodElement, mb: MethodElement)                     => compareMethodElement(ma, mb)
        case (ua: UnaryExpressionElement, ub: UnaryExpressionElement)   => compareUnary(ua, ub)
        case (ba: BinaryExpressionElement, bb: BinaryExpressionElement) => compareBinary(ba, bb)
        case (pa: ParameterElement, pb: ParameterElement)               => compareParameter(pa, pb)
        case (ma: MemberElement, mb: MemberElement)                     => compareMemberAccess(ma, mb)
        case (ca: ConstValueElement, cb: ConstValueElement)             => compareConstant(ca, cb)
        case (fa: FuncElement, fb: FuncElement)                         => compareFuncCall(fa, fb)
        case (ea: ExpressionListElement, eb: ExpressionListElement)     => compareExpressionList(ea, eb)
        case (pa: ProviderRelation, pb: ProviderRelation)               => compareProvider(pa, pb)
        case (na: NewElement, nb: NewElement)                           => compareNew(na, nb)
        case (la: LambdaElement, lb: LambdaElement)                     => compareLambda(la, lb)
        case (ca: ConditionalElement, cb: ConditionalElement)           => compareConditional(ca, cb)
        case (ta: TypeIsElement, tb: TypeIsElement)                     => compareTypeIs(ta, tb)
        case _ =>
          throw new RelationalUnsupportedException(s"Unhandled element type: ${a.elementType}")
      }
  }

  protected def compareTypeIs(a: TypeIsElement, b: TypeIsElement): Boolean = {
    compare(a.element, b.element) && (a.targetType.classes.toSet == b.targetType.classes.toSet)
  }

  protected def compareMethodElement(a: MethodElement, b: MethodElement): Boolean = {
    a.methodCode == b.methodCode && compareMethodArgs(a.methodArgs, b.methodArgs)
  }

  protected def compareUnary(a: UnaryExpressionElement, b: UnaryExpressionElement): Boolean = {
    a.op == b.op && compare(a.element, b.element)
  }

  protected def compareBinary(a: BinaryExpressionElement, b: BinaryExpressionElement): Boolean = {
    a.op == b.op && compareMemberDescriptor(a.method, b.method) && compare(a.left, b.left) && compare(a.right, b.right)
  }

  protected def compareParameter(a: ParameterElement, b: ParameterElement): Boolean = {
    if (paramScope ne null) {
      val mapped = paramScope.getOrElse(a, null)
      if (mapped ne null) mapped eq b else a eq b
    } else a eq b
  }

  protected def compareMemberAccess(a: MemberElement, b: MemberElement): Boolean = {
    a.memberName == b.memberName && compare(a.instanceProvider, b.instanceProvider)
  }

  protected def compareConstant(a: ConstValueElement, b: ConstValueElement): Boolean = {
    if (fnCompare ne null) fnCompare(a.value, b.value)
    else a.value == b.value
  }

  protected def compareFuncCall(a: FuncElement, b: FuncElement): Boolean = {
    val sameMethod = (a.callee, b.callee) match {
      case (ca: MethodCallee, cb: MethodCallee) => compareMemberDescriptor(ca.method, cb.method)
      case _                                    => false
    }
    sameMethod && compare(a.instance, b.instance) && compareElementList(a.arguments, b.arguments)
  }

  protected def compareExpressionList(a: ExpressionListElement, b: ExpressionListElement): Boolean = {
    compareElementList(a.exprList, b.exprList)
  }

  protected def compareProvider(pa: ProviderRelation, pb: ProviderRelation): Boolean = {
    pa.getClass == pb.getClass && (if (fnCompare ne null) fnCompare(pa, pb) else pa == pb)
  }

  protected def compareNew(a: NewElement, b: NewElement): Boolean = {
    compareMemberDescriptor(a.ctor, b.ctor) &&
    compareElementList(a.arguments, b.arguments) &&
    compareMemberDescriptors(a.members, b.members)
  }

  protected def compareLambda(a: LambdaElement, b: LambdaElement): Boolean = {
    if (a.parameters.size != b.parameters.size) false
    else if (!a.parameters.zip(b.parameters).forall(p => p._1.rowTypeInfo == p._2.rowTypeInfo)) false
    else {
      val save = paramScope
      paramScope = new ScopedHashMap(save)
      try {
        for ((pa, pb) <- a.parameters.zip(b.parameters)) {
          paramScope.put(pa, pb)
        }
        compare(a.body, b.body)
      } finally {
        paramScope = save
      }
    }
  }

  protected def compareConditional(a: ConditionalElement, b: ConditionalElement): Boolean = {
    compare(a.test, b.test) && compare(a.ifTrue, b.ifTrue) && compare(a.ifFalse, b.ifFalse)
  }

  protected def compareMemberDescriptor(a: MemberDescriptor, b: MemberDescriptor): Boolean = {
    if (a eq b) true
    else if (a == null || b == null) false
    else {
      a.memberType == b.memberType && a.name == b.name && a.declaringType == b.declaringType
    }
  }

  protected def compareMemberDescriptors(a: List[MemberDescriptor], b: List[MemberDescriptor]): Boolean = {
    if (a eq b) true
    else if (a == null || b == null) false
    else if (a.size != b.size) false
    else {
      a.zip(b).forall(t => compareMemberDescriptor(t._1, t._2))
    }
  }

  protected def compareMethodArgs(a: List[MethodArg[RelationElement]], b: List[MethodArg[RelationElement]]): Boolean = {
    if (a eq b) true
    else if (a == null || b == null) false
    else if (a.size != b.size) false
    else {
      a.zip(b).forall { case (argA, argB) =>
        argA.name == argB.name && compare(argA.arg, argB.arg)
      }
    }
  }

  protected def compareElementList(a: List[RelationElement], b: List[RelationElement]): Boolean = {
    if (a eq b) true
    else if (a == null || b == null) false
    else if (a.size != b.size) false
    else {
      a.zip(b).forall(t => compare(t._1, t._2))
    }
  }
}
