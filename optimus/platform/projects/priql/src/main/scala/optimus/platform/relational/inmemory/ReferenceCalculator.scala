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

import java.util.IdentityHashMap

import optimus.platform.relational.tree._

import scala.collection.mutable

/**
 * Calculate the reference count of a RelationElement
 */
class ReferenceCalculator extends QueryTreeVisitor {
  protected val visited = new java.util.HashMap[RelationElement, Int]

  override def visitElement(element: RelationElement): RelationElement = {
    element match {
      case e @ (_: ProviderRelation | _: MethodElement) =>
        if (visited.containsKey(e)) {
          val refCount = visited.get(e)
          visited.put(e, refCount + 1)
        } else {
          visited.put(e, 1)
          super.visitElement(element)
        }
        e
      case _ => super.visitElement(element)
    }
  }
}
object ReferenceCalculator {
  def calculate(e: RelationElement): mutable.HashMap[RelationElement, Int] = {
    val calculator = new ReferenceCalculator()
    calculator.visitElement(e)
    val result = new mutable.HashMap[RelationElement, Int]
    val it = calculator.visited.entrySet.iterator
    while (it.hasNext) {
      val item = it.next
      if (item.getValue != 1) result.put(item.getKey, item.getValue)
    }
    result
  }
}
