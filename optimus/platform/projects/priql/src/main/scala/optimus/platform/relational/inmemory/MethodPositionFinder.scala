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

import optimus.platform.relational.tree.MethodElement
import optimus.platform.relational.tree.MethodPosition
import optimus.platform.relational.tree.ProviderRelation
import optimus.platform.relational.tree.QueryTreeVisitor
import optimus.platform.relational.tree.RelationElement

class MethodPositionFinder extends QueryTreeVisitor {
  private var pos: MethodPosition = MethodPosition.unknown

  override protected def handleQuerySrc(element: ProviderRelation): RelationElement = {
    handleMethodPosition(element.pos)
    element
  }

  override protected def handleMethod(method: MethodElement): RelationElement = {
    if (handleMethodPosition(method.pos)) method else super.handleMethod(method)
  }

  private def handleMethodPosition(position: MethodPosition): Boolean = {
    if ((pos eq MethodPosition.unknown) && (position ne MethodPosition.unknown)) {
      pos = position
      true
    } else false
  }
}

object MethodPositionFinder {
  def find(e: RelationElement): MethodPosition = {
    val f = new MethodPositionFinder
    f.visitElement(e)
    f.pos
  }
}
