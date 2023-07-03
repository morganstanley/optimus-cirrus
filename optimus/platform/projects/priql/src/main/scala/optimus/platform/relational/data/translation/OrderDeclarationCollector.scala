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

import optimus.platform.relational.data.tree.{DbQueryTreeVisitor, OrderDeclaration, SortDirection, TupleElement}
import optimus.platform.relational.tree.{FuncElement, MethodCallee, RelationElement}

import scala.collection.mutable.ListBuffer

private class OrderDeclarationCollector extends DbQueryTreeVisitor {
  val listBuffer = new ListBuffer[OrderDeclaration]
  private var savedDirection: SortDirection = SortDirection.Ascending

  override def visitElement(e: RelationElement): RelationElement = {
    e match {
      case TupleElement(elements) =>
        elements.foreach(visitElement)

      case FuncElement(c: MethodCallee, args, null) if c.resType ne null =>
        val moduleName = c.resType.runtimeClass.getName()
        if (moduleName == "optimus.platform.relational.asc")
          visitElement(args.head)
        else if (moduleName == "optimus.platform.relational.desc") {
          val preOrder = savedDirection
          savedDirection =
            if (preOrder == SortDirection.Ascending) SortDirection.Descending else SortDirection.Ascending
          visitElement(args.head)
          savedDirection = preOrder
        }

      case _ => listBuffer += new OrderDeclaration(savedDirection, e)
    }
    e
  }
}

object OrderDeclarationCollector {
  def collect(element: RelationElement): List[OrderDeclaration] = {
    val collector = new OrderDeclarationCollector()
    collector.visitElement(element)
    collector.listBuffer.result()
  }
}
