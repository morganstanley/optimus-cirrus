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

import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree.RelationElement
import scala.collection.mutable.HashSet

class NamedValueGatherer private () extends DbQueryTreeVisitor {
  private val namedValues = new HashSet[NamedValueWrapper]

  protected override def handleNamedValue(value: NamedValueElement): RelationElement = {
    namedValues.add(new NamedValueWrapper(value))
    value
  }
}

object NamedValueGatherer {
  def gather(e: RelationElement): List[NamedValueElement] = {
    val gatherer = new NamedValueGatherer()
    gatherer.visitElement(e)
    gatherer.namedValues.iterator.map(_.v).toList
  }
}

private class NamedValueWrapper(val v: NamedValueElement) {
  override def equals(o: Any): Boolean = {
    val other = o.asInstanceOf[NamedValueWrapper]
    (other ne null) && v.name == other.v.name
  }

  override def hashCode: Int = {
    v.name.hashCode()
  }
}
