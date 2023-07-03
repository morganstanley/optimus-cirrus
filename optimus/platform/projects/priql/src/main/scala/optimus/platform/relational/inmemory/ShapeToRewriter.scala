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
package optimus.platform.relational.inmemory

import optimus.platform.relational.execution.NonRepetitiveMethodTreeVisitor
import optimus.platform.relational.tree._

/**
 * ReWrite shape before untype. Keep all column info from sourceType.
 */
class ShapeToRewriter extends NonRepetitiveMethodTreeVisitor {

  protected override def handleMethod(method: MethodElement): RelationElement = {
    method.methodCode match {
      case QueryMethod.UNTYPE =>
        method.methodArgs.head.param match {
          case MethodElement(QueryMethod.SHAPE, methodArgs, projectedType, _, pos) =>
            val (src :: others) = methodArgs
            val newSrc = visitElement(src.param)
            val args = MethodArg("src", newSrc) +: others :+ MethodArg[RelationElement](
              "shapeToType",
              new ConstValueElement(projectedType))
            new MethodElement(QueryMethod.SHAPE, args, method.projectedType(), method.key, pos)

          case _ => super.handleMethod(method)
        }

      case _ => super.handleMethod(method)
    }
  }

}

object ShapeToRewriter {
  def rewrite(e: RelationElement): RelationElement = {
    new ShapeToRewriter().visitElement(e)
  }
}
