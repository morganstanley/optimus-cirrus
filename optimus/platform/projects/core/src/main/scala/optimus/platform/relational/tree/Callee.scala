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

import optimus.graph.Node

import scala.util.Try

class Callee(val resType: TypeInfo[_], val name: String, val signature: String, val arguments: List[Argument]) {}

class Argument(val argType: TypeInfo[_]) {}

class MethodCallee(val method: MethodDescriptor)
    extends Callee(method.returnType, method.name, null, method.getParameters().map(p => new Argument(p.typeInfo))) {}

final case class ScalaLambdaCallee[A, R](
    val lambda: Either[A => Node[R], A => R],
    val lambdaFunc: Option[() => Try[LambdaElement]],
    override val resType: TypeInfo[R],
    override val name: String,
    override val signature: String,
    override val arguments: List[Argument])
    extends Callee(resType, name, signature, arguments) {

  def this(
      lam: Either[A => Node[R], A => R],
      lambdaFunc: Option[() => Try[LambdaElement]],
      res: TypeInfo[R],
      args: List[Argument]) = this(lam, lambdaFunc, res, null, null, args)
  def this(
      lam: Either[A => Node[R], A => R],
      lambdaFunc: Option[() => Try[LambdaElement]],
      res: TypeInfo[R],
      arg: Argument) = this(lam, lambdaFunc, res, null, null, List(arg))
  def this(lam: Either[A => Node[R], A => R], lambdaFunc: Option[() => Try[LambdaElement]]) =
    this(lam, lambdaFunc, null, null, null, List.empty)

  def lambdaElement: Option[LambdaElement] = lambdaFunc.flatMap(f => f().toOption)
}

final case class ScalaLambdaCallee2[A, B, R](
    val lambda: Either[(A, B) => Node[R], (A, B) => R],
    val lambdaFunc: Option[() => Try[LambdaElement]],
    override val resType: TypeInfo[R],
    override val name: String,
    override val signature: String,
    override val arguments: List[Argument])
    extends Callee(resType, name, signature, arguments) {

  def this(
      lam: Either[(A, B) => Node[R], (A, B) => R],
      lambdaFunc: Option[() => Try[LambdaElement]],
      res: TypeInfo[R],
      args: List[Argument]) = this(lam, lambdaFunc, res, null, null, args)
  def this(
      lam: Either[(A, B) => Node[R], (A, B) => R],
      lambdaFunc: Option[() => Try[LambdaElement]],
      res: TypeInfo[R],
      arg: Argument) = this(lam, lambdaFunc, res, null, null, List(arg))
  def this(lam: Either[(A, B) => Node[R], (A, B) => R], lambdaFunc: Option[() => Try[LambdaElement]]) =
    this(lam, lambdaFunc, null, null, null, List.empty)

  def lambdaElement: Option[LambdaElement] = lambdaFunc.flatMap(f => f().toOption)
}

object FuncCode extends Enumeration {
  type FuncCode = Value

  val INVALID = Value("INVALID")
  val COUNT = Value("COUNT")
  val SUM = Value("SUM")
  val MIN = Value("MIN")
  val MAX = Value("MAX")
  val AVG = Value("AVG")
}
