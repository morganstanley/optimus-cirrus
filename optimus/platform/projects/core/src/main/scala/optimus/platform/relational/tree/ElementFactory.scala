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
import optimus.platform.relational.tree.BinaryExpressionType._

/**
 * The ElementFactory object is used to construct the element nodes in the query tree of PriQL.
 */
object ElementFactory {
  def andAlso(left: RelationElement, right: RelationElement): BinaryExpressionElement =
    new LogicalBinaryExpressionElement(BinaryExpressionType.BOOLAND, left, right)

  def orElse(left: RelationElement, right: RelationElement): BinaryExpressionElement =
    new LogicalBinaryExpressionElement(BinaryExpressionType.BOOLOR, left, right)

  def add(left: RelationElement, right: RelationElement): BinaryExpressionElement =
    add(left, right, TypeDescriptor.getMethod(left.rowTypeInfo, "$plus", right.rowTypeInfo :: Nil))

  private[tree] def add(
      left: RelationElement,
      right: RelationElement,
      method: MethodDescriptor): BinaryExpressionElement =
    new MethodBinaryExpressionElement(BinaryExpressionType.PLUS, left, right, method)

  def subtract(left: RelationElement, right: RelationElement): BinaryExpressionElement =
    subtract(left, right, TypeDescriptor.getMethod(left.rowTypeInfo, "$minus", right.rowTypeInfo :: Nil))

  private[tree] def subtract(
      left: RelationElement,
      right: RelationElement,
      method: MethodDescriptor): BinaryExpressionElement =
    new MethodBinaryExpressionElement(BinaryExpressionType.MINUS, left, right, method)

  def multiply(left: RelationElement, right: RelationElement): BinaryExpressionElement =
    multiply(left, right, TypeDescriptor.getMethod(left.rowTypeInfo, "$times", right.rowTypeInfo :: Nil))

  private[tree] def multiply(
      left: RelationElement,
      right: RelationElement,
      method: MethodDescriptor): BinaryExpressionElement =
    new MethodBinaryExpressionElement(BinaryExpressionType.MUL, left, right, method)

  def divide(left: RelationElement, right: RelationElement): BinaryExpressionElement =
    divide(left, right, TypeDescriptor.getMethod(left.rowTypeInfo, "$div", right.rowTypeInfo :: Nil))

  private[tree] def divide(
      left: RelationElement,
      right: RelationElement,
      method: MethodDescriptor): BinaryExpressionElement =
    new MethodBinaryExpressionElement(BinaryExpressionType.DIV, left, right, method)

  def modulo(left: RelationElement, right: RelationElement): BinaryExpressionElement =
    modulo(left, right, TypeDescriptor.getMethod(left.rowTypeInfo, "$percent", right.rowTypeInfo :: Nil))

  private[tree] def modulo(
      left: RelationElement,
      right: RelationElement,
      method: MethodDescriptor): BinaryExpressionElement =
    new MethodBinaryExpressionElement(BinaryExpressionType.MODULO, left, right, method)

  def equal(left: RelationElement, right: RelationElement): BinaryExpressionElement =
    equal(left, right, TypeDescriptor.getMethod(left.rowTypeInfo, "$eq$eq", right.rowTypeInfo :: Nil))

  private[tree] def equal(
      left: RelationElement,
      right: RelationElement,
      method: MethodDescriptor): BinaryExpressionElement =
    if (method != null && TypeInfo.BOOLEAN == method.returnType)
      new MethodBinaryExpressionElement(BinaryExpressionType.EQ, left, right, method)
    else new BinaryExpressionElement(BinaryExpressionType.EQ, left, right, TypeInfo.BOOLEAN)

  def notEqual(left: RelationElement, right: RelationElement): BinaryExpressionElement =
    notEqual(left, right, TypeDescriptor.getMethod(left.rowTypeInfo, "$bang$eq", right.rowTypeInfo :: Nil))

  private[tree] def notEqual(
      left: RelationElement,
      right: RelationElement,
      method: MethodDescriptor): BinaryExpressionElement =
    if (method != null && method.returnType == TypeInfo.BOOLEAN)
      new MethodBinaryExpressionElement(BinaryExpressionType.NE, left, right, method)
    else new BinaryExpressionElement(BinaryExpressionType.NE, left, right, TypeInfo.BOOLEAN)

  def greaterThan(left: RelationElement, right: RelationElement): BinaryExpressionElement =
    greaterThan(left, right, TypeDescriptor.getMethod(left.rowTypeInfo, "$greater", right.rowTypeInfo :: Nil))

  private[tree] def greaterThan(
      left: RelationElement,
      right: RelationElement,
      method: MethodDescriptor): BinaryExpressionElement =
    if (method != null && method.returnType == TypeInfo.BOOLEAN)
      new MethodBinaryExpressionElement(BinaryExpressionType.GT, left, right, method)
    else new BinaryExpressionElement(BinaryExpressionType.GT, left, right, TypeInfo.BOOLEAN)

  def greaterThanOrEqual(left: RelationElement, right: RelationElement): BinaryExpressionElement =
    greaterThanOrEqual(left, right, TypeDescriptor.getMethod(left.rowTypeInfo, "$greater$eq", right.rowTypeInfo :: Nil))

  private[tree] def greaterThanOrEqual(
      left: RelationElement,
      right: RelationElement,
      method: MethodDescriptor): BinaryExpressionElement =
    if (method != null && method.returnType == TypeInfo.BOOLEAN)
      new MethodBinaryExpressionElement(BinaryExpressionType.GE, left, right, method)
    else new BinaryExpressionElement(BinaryExpressionType.GE, left, right, TypeInfo.BOOLEAN)

  def lessThan(left: RelationElement, right: RelationElement): BinaryExpressionElement =
    lessThan(left, right, TypeDescriptor.getMethod(left.rowTypeInfo, "$less", right.rowTypeInfo :: Nil))

  private[tree] def lessThan(
      left: RelationElement,
      right: RelationElement,
      method: MethodDescriptor): BinaryExpressionElement =
    if (method != null && method.returnType == TypeInfo.BOOLEAN)
      new MethodBinaryExpressionElement(BinaryExpressionType.LT, left, right, method)
    else new BinaryExpressionElement(BinaryExpressionType.LT, left, right, TypeInfo.BOOLEAN)

  def lessThanOrEqual(left: RelationElement, right: RelationElement): BinaryExpressionElement =
    lessThanOrEqual(left, right, TypeDescriptor.getMethod(left.rowTypeInfo, "$less$eq", right.rowTypeInfo :: Nil))

  private[tree] def lessThanOrEqual(
      left: RelationElement,
      right: RelationElement,
      method: MethodDescriptor): BinaryExpressionElement =
    if (method != null && method.returnType == TypeInfo.BOOLEAN)
      new MethodBinaryExpressionElement(BinaryExpressionType.LE, left, right, method)
    else new BinaryExpressionElement(BinaryExpressionType.LE, left, right, TypeInfo.BOOLEAN)

  def call(instance: RelationElement, method: MethodDescriptor, arguments: List[RelationElement]): FuncElement =
    new FuncElement(new MethodCallee(method), arguments, instance)

  def condition(
      test: RelationElement,
      ifTrue: RelationElement,
      ifFalse: RelationElement,
      typeDescriptor: TypeInfo[_]): ConditionalElement =
    new ConditionalElement(test, ifTrue, ifFalse, typeDescriptor)

  def constant(value: Any): ConstValueElement = new ConstValueElement(value, TypeInfo.generic(value))

  def constant(value: Any, typeDescriptor: TypeInfo[_]): ConstValueElement =
    new ConstValueElement(value, typeDescriptor)

  def lambda(body: RelationElement, parameters: collection.Seq[ParameterElement]): LambdaElement =
    new LambdaElement(body.rowTypeInfo, body, parameters)

  def makeBinary(
      binaryType: BinaryExpressionType,
      left: RelationElement,
      right: RelationElement): BinaryExpressionElement = binaryType match {
    case BinaryExpressionType.BOOLAND => andAlso(left, right)
    case BinaryExpressionType.BOOLOR  => orElse(left, right)
    case BinaryExpressionType.PLUS    => add(left, right)
    case BinaryExpressionType.MINUS   => subtract(left, right)
    case BinaryExpressionType.DIV     => divide(left, right)
    case BinaryExpressionType.MUL     => multiply(left, right)
    case BinaryExpressionType.MODULO  => modulo(left, right)
    case BinaryExpressionType.EQ      => equal(left, right)
    case BinaryExpressionType.NE      => notEqual(left, right)
    case BinaryExpressionType.GT      => greaterThan(left, right)
    case BinaryExpressionType.GE      => greaterThanOrEqual(left, right)
    case BinaryExpressionType.LT      => lessThan(left, right)
    case BinaryExpressionType.LE      => lessThanOrEqual(left, right)
    case BinaryExpressionType.ITEM_IS_IN =>
      new BinaryExpressionElement(binaryType, left, right, TypeInfo.BOOLEAN)
    case BinaryExpressionType.ITEM_IS_NOT_IN =>
      new BinaryExpressionElement(binaryType, left, right, TypeInfo.BOOLEAN)
    case _ => throw new RelationalUnsupportedException("Unsupported bianryType: " + binaryType.toString)
  }

  def makeBinary(
      binaryType: BinaryExpressionType,
      left: RelationElement,
      right: RelationElement,
      method: MethodDescriptor): BinaryExpressionElement = binaryType match {
    case BinaryExpressionType.BOOLAND => andAlso(left, right)
    case BinaryExpressionType.BOOLOR  => orElse(left, right)
    case BinaryExpressionType.PLUS    => add(left, right, method)
    case BinaryExpressionType.MINUS   => subtract(left, right, method)
    case BinaryExpressionType.DIV     => divide(left, right, method)
    case BinaryExpressionType.MUL     => multiply(left, right, method)
    case BinaryExpressionType.MODULO  => modulo(left, right, method)
    case BinaryExpressionType.EQ      => equal(left, right, method)
    case BinaryExpressionType.NE      => notEqual(left, right, method)
    case BinaryExpressionType.GT      => greaterThan(left, right, method)
    case BinaryExpressionType.GE      => greaterThanOrEqual(left, right, method)
    case BinaryExpressionType.LT      => lessThan(left, right, method)
    case BinaryExpressionType.LE      => lessThanOrEqual(left, right, method)
    case BinaryExpressionType.ITEM_IS_IN if method ne null =>
      new MethodBinaryExpressionElement(binaryType, left, right, method)
    case _ => throw new RelationalUnsupportedException("Unsupported binaryType: " + binaryType.toString)
  }

  def convert(element: RelationElement, targetType: TypeInfo[_]): RelationElement = {
    new UnaryExpressionElement(UnaryExpressionType.CONVERT, element, targetType)
  }

  def makeMemberAccess(expression: RelationElement, member: MemberDescriptor): MemberElement =
    new MemberElement(expression, member.name, member)

  def makeNew(
      ctor: ConstructorDescriptor,
      arguments: List[RelationElement],
      members: List[MemberDescriptor]): NewElement = new NewElement(ctor, arguments, members)

  def parameter(name: String): ParameterElement = new ParameterElement(name)

  def parameter(name: String, typeDescriptor: TypeInfo[_]): ParameterElement =
    new ParameterElement(name, typeDescriptor)

  def typeIs(element: RelationElement, targetType: TypeInfo[_]): TypeIsElement =
    new TypeIsElement(element, targetType)
}
