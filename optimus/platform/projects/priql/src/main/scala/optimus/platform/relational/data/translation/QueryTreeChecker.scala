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
package optimus.platform.relational.data.translation

import optimus.platform.relational.data.tree.DbQueryTreeVisitor
import optimus.platform.relational.tree.RelationElement

class QueryTreeChecker private (val f: RelationElement => Boolean) extends DbQueryTreeVisitor {
  var violation: RelationElement = _

  override def visitElement(element: RelationElement): RelationElement = {
    if ((violation ne null) || (element eq null))
      element
    else if (f(element))
      super.visitElement(element)
    else {
      violation = element
      element
    }
  }
}

object QueryTreeChecker {
  def forall(element: RelationElement, f: RelationElement => Boolean): Boolean = {
    val checker = new QueryTreeChecker(f)
    checker.visitElement(element)
    checker.violation eq null
  }
}
