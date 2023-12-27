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

import optimus.platform.RelationKey
import optimus.platform.NoKey
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.data.DbQueryTreeReducerBase
import optimus.platform.relational.data.QueryCommand
import optimus.platform.relational.data.mapping.MappingEntityLookup
import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree._
import optimus.platform.relational.tree.RelationElement

class DALFullTextSearchReducer(val provider: DALProvider) extends DbQueryTreeReducerBase {
  def createMapping() = new FullTextSearchMapping
  def createLanguage(lookup: MappingEntityLookup) = new DALFullTextSearchLanguage(lookup)

  protected override def buildInner(e: RelationElement): RelationElement = {
    throw new RelationalUnsupportedException("Inner query is not supported")
  }

  protected override def compileAndExecute(
      command: QueryCommand,
      projector: LambdaElement,
      proj: ProjectionElement): RelationElement = {
    def executeDALHeapEntityElement(d: DALHeapEntityElement, key: RelationKey[_]): RelationElement = {
      val fn = DALProvider.knownProjectorToLambda(d).get
      provider.execute(command, fn, key, d.rowTypeInfo, executeOptions.asEntitledOnlyIf(proj.entitledOnly))
    }

    proj.projector match {
      case d: DALHeapEntityElement =>
        executeDALHeapEntityElement(d, proj.key)
      case pj if proj.select.columns.size == 1 =>
        DALProvider.knownProjectorToLambda(pj) map { fn =>
          provider.execute(command, fn, proj.key, pj.rowTypeInfo, executeOptions.asEntitledOnlyIf(proj.entitledOnly))
        } getOrElse {
          super.compileAndExecute(command, projector, proj)
        }
      case _ =>
        super.compileAndExecute(command, projector, proj)
    }
  }

  protected override def handleDALHeapEntity(de: DALHeapEntityElement): RelationElement = {
    de.members.head match {
      case e: ColumnElement => getReaderFunction(e, de.rowTypeInfo).getOrElse(e)
    }
  }
}
