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

import optimus.platform.relational.data.tree.ColumnElement
import optimus.platform.relational.data.tree.DALHeapEntityElement
import optimus.platform.relational.data.tree.DbQueryTreeVisitor
import optimus.platform.relational.data.tree.EmbeddableCaseClassElement
import optimus.platform.relational.tree.RelationElement

import scala.collection.mutable

object ColumnCollector {
  def collect(element: RelationElement): Set[RelationElement] = {
    val c = new ColumnCollector()
    c.visitElement(element)
    c.elements.toSet
  }
}

// This class is to collect columns used in projector.
class ColumnCollector extends DbQueryTreeVisitor {
  val elements = new mutable.HashSet[RelationElement]

  protected override def handleColumn(column: ColumnElement): RelationElement = {
    elements += column
    column
  }

  protected override def handleDALHeapEntity(entity: DALHeapEntityElement): RelationElement = {
    elements += entity
    entity
  }

  protected override def handleEmbeddableCaseClass(caseClass: EmbeddableCaseClassElement): RelationElement = {
    elements += caseClass
    caseClass
  }
}
