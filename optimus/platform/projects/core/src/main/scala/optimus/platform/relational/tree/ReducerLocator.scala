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

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * ReducerLocator is a kind of QueryTreeVisitor. It will traverse every node in the query tree to find all the reducers
 * given by data source provider and store them into an array.
 */
class ReducerLocator(category: ExecutionCategory) extends QueryTreeVisitor {

  private[this] val reducers = new ListBuffer[ReducerVisitor]()
  private[this] val visited = new mutable.HashSet[Long]
  val base = RelationElementCnt.prev // keep the numbers small for purely aesthetic reasons.

  def locateReduces(element: RelationElement): List[ReducerVisitor] = {
    visitElement(element)
    reducers.result()
  }

  override def handleQuerySrc(p: ProviderRelation): RelationElement = {
    val i = p.serial - base
    if (!visited.contains(i)) {
      visited += i
      reducers ++= p.reducersFor(category)
    }
    p
  }
}

object ReducerLocator {
  def locate(element: RelationElement, category: ExecutionCategory): List[ReducerVisitor] = {
    new ReducerLocator(category).locateReduces(element)
  }
}
