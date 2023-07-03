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
import optimus.platform.relational.data.tree.OptionElement
import optimus.platform.relational.tree.ConditionalElement
import optimus.platform.relational.tree.ConstValueElement
import optimus.platform.relational.tree.ElementFactory
import optimus.platform.relational.tree.RelationElement

class ConditionalFlattener extends DbQueryTreeVisitor {
  protected override def handleConditional(c: ConditionalElement): RelationElement = {
    def flatten(cond: ConditionalElement): ConditionalElement = {
      // Option(Option(...Option( cond )...)) when cond matches ConditionalElement(_, ConstValueElement(null, _), _, _)
      OptionElement.underlying(cond.ifFalse) match {
        case c @ ConditionalElement(_, ConstValueElement(null, _), _, _) =>
          val flattened = flatten(c)
          val ifFalse = OptionElement.wrapWithOptionElement(cond.ifFalse, flattened.ifFalse)
          ElementFactory.condition(
            ElementFactory.orElse(cond.test, flattened.test),
            cond.ifTrue,
            ifFalse,
            cond.rowTypeInfo)
        case _ =>
          cond
      }
    }
    super.handleConditional(c) match {
      case x @ ConditionalElement(_, ConstValueElement(null, _), _, _) => flatten(x)
      case x =>
        x
    }
  }
}

object ConditionalFlattener {
  def flatten(e: RelationElement): RelationElement = {
    new ConditionalFlattener().visitElement(e)
  }
}
