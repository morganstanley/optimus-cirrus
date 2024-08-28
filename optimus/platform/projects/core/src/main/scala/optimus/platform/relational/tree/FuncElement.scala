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

/**
 * FuncElement represents function call such as method call or scala lambda.
 */
final class FuncElement(val callee: Callee, val arguments: List[RelationElement], val instance: RelationElement)
    extends RelationElement(ElementType.ForteFuncCall, callee.resType) {

  override def prettyPrint(indent: Int, out: StringBuilder): Unit = {
    indentAndStar(indent, out)

    out ++= s"${serial} FuncCall: '${callee.getClass.getSimpleName}' ("
    out ++= (if (callee.name != null) callee.name else "f") + ": ("
    if (callee.arguments != null) {
      out ++= (callee.arguments map { _.argType.name }) mkString (", ")
    }

    out ++= (if (callee.resType == null) " ))\n" else s" ) => ${callee.resType.name})\n")

  }
}

object FuncElement {
  def convertFuncElementToLambda[A, R](element: RelationElement): Either[A => Node[R], A => R] = {
    element match {
      case FuncElement(callee, _, _) =>
        callee match {
          case mapLambda: ScalaLambdaCallee[A, R] @unchecked => mapLambda.lambda
          case _ => throw new RuntimeException("Only Support Scala compiled lambda")
        }
      case _ => throw new RuntimeException("Only Support Scala compiled lambda")
    }
  }

  def convertFuncElementToLambda2[A, B, R](element: RelationElement): Either[(A, B) => Node[R], (A, B) => R] = {
    element match {
      case FuncElement(callee, _, _) =>
        callee match {
          case mapLambda: ScalaLambdaCallee2[A, B, R] @unchecked => mapLambda.lambda
          case _ => throw new RuntimeException("Only Support Scala compiled lambda")
        }
      case _ => throw new RuntimeException("Only Support Scala compiled lambda")
    }
  }

  def getScalaLambdaCallee[A, R](element: RelationElement): ScalaLambdaCallee[A, R] = {
    element match {
      case FuncElement(callee, _, _) =>
        callee match {
          case mapLambda: ScalaLambdaCallee[A, R] @unchecked => mapLambda
          case _ => throw new RuntimeException("Only Support Scala compiled lambda")
        }
      case _ => throw new RuntimeException("Only Support Scala compiled lambda")
    }
  }

  def getScalaLambdaCallee2[A, B, R](element: RelationElement): ScalaLambdaCallee2[A, B, R] = {
    element match {
      case FuncElement(callee, _, _) =>
        callee match {
          case mapLambda: ScalaLambdaCallee2[A, B, R] @unchecked => mapLambda
          case _ => throw new RuntimeException("Only Support Scala compiled lambda")
        }
      case _ => throw new RuntimeException("Only Support Scala compiled lambda")
    }
  }

  def unapply(element: FuncElement) = Some(element.callee, element.arguments, element.instance)
}
