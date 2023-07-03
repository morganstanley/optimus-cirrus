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

import scala.collection.mutable.HashMap
import optimus.platform.relational.tree.{MultiRelationElement, ElementType, QueryTreeVisitor, RelationElement}

/*
 * 1. When we collect reducer, we won't collect the identical one multiple times.
 *
 * 2. When we reduce the tree, we won't want to reduce the same element multiple times.
 *
 * 3. When we create ExecNode, we won't want to map a same RelationElement to multiple ExecNode.
 */
abstract class NonRepetitiveMethodTreeVisitor extends QueryTreeVisitor {
  protected val visited = new HashMap[RelationElement, RelationElement]

  override def visitElement(element: RelationElement): RelationElement = {
    if (element eq null) null
    else {
      element match {
        case e: MultiRelationElement           => visited.getOrElseUpdate(element, super.visitElement(element))
        case e if e.elementType == RuntimeExec => element
        case _                                 => super.visitElement(element)
      }
    }
  }
}
