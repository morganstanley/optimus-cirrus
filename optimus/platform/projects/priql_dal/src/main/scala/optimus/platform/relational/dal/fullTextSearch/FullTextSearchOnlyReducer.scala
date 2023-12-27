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
package optimus.platform.relational.dal.fullTextSearch

import optimus.platform.relational.PriqlSettings
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.dal.DALQueryMethod._
import optimus.platform.relational.tree._

class FullTextSearchOnlyReducer(val reducer: ReducerVisitor) extends QueryTreeVisitor with ReducerVisitor {
  private[this] var insideFullTextSearchOnly = false

  override def reduce(tree: RelationElement, executeOptions: ExecuteOptions): RelationElement = {
    reducer.reduce(visitElement(tree))
  }

  protected override def handleMethod(method: MethodElement): RelationElement = {
    method.methodCode match {
      case FullTextSearchOnly =>
        val savedInsideFullTextSearchOnly = insideFullTextSearchOnly
        insideFullTextSearchOnly = true
        val element = super.handleMethod(method)
        insideFullTextSearchOnly = savedInsideFullTextSearchOnly
        element
      case _ =>
        super.handleMethod(method)
    }
  }

  protected override def handleQuerySrc(element: ProviderRelation): RelationElement = {
    element match {
      case p: DALProvider if insideFullTextSearchOnly =>
        require(p.canBeFullTextSearch, s"$p cannot be within 'fullTextSearchOnly' scope")
        p
      case p: DALProvider =>
        if (!p.canBeFullTextSearch || PriqlSettings.enableFullTextSearchPriql) p
        else
          new DALProvider(p.classEntityInfo, p.rowTypeInfo, p.key, p.pos, p.dalApi, p.canBeProjected, false, p.keyPolicy, p.entitledOnly)
      case p =>
        require(!insideFullTextSearchOnly, s"$p cannot be within 'fullTextSearchOnly' scope")
        p
    }
  }
}
