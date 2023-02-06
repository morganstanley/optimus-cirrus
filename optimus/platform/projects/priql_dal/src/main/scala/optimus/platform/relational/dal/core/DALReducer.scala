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
package optimus.platform.relational.dal.core

import optimus.platform.NoKey
import optimus.platform.RelationKey
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.data.DbQueryTreeReducerBase
import optimus.platform.relational.data.QueryCommand
import optimus.platform.relational.data.language.FormattedQuery
import optimus.platform.relational.data.mapping.MappingEntityLookup
import optimus.platform.relational.data.mapping.QueryMapping
import optimus.platform.relational.data.tree.ColumnElement
import optimus.platform.relational.data.tree.DALHeapEntityElement
import optimus.platform.relational.data.tree.DynamicObjectElement
import optimus.platform.relational.data.tree.ProjectionElement
import optimus.platform.relational.tree.LambdaElement
import optimus.platform.relational.tree.MethodArg
import optimus.platform.relational.tree.MethodElement
import optimus.platform.relational.tree.QueryMethod
import optimus.platform.relational.tree.RelationElement
import optimus.platform.relational.tree.TypeInfo

class DALReducer(val provider: DALProvider) extends DbQueryTreeReducerBase {

  def createMapping(): QueryMapping = new DALMapping
  def createLanguage(lookup: MappingEntityLookup) = new DALLanguage(lookup)

  protected override def buildInner(e: RelationElement): RelationElement = {
    throw new RelationalUnsupportedException("Inner query is not supported")
  }

  protected override def compileAndExecute(
      command: QueryCommand,
      projector: LambdaElement,
      proj: ProjectionElement): RelationElement = {
    def executeDALHeapEntityElement(d: DALHeapEntityElement, key: RelationKey[_]): RelationElement = {
      provider.execute(
        command,
        Right(DALProvider.getEntity),
        key,
        d.rowTypeInfo,
        executeOptions.asEntitledOnlyIf(proj.entitledOnly))
    }

    proj.projector match {
      case d: DALHeapEntityElement =>
        executeDALHeapEntityElement(d, proj.key)
      case DynamicObjectElement(d: DALHeapEntityElement) =>
        new MethodElement(
          QueryMethod.UNTYPE,
          MethodArg("src", executeDALHeapEntityElement(d, NoKey)) :: Nil,
          proj.rowTypeInfo,
          proj.key)
      case c: ColumnElement if c.rowTypeInfo == TypeInfo.LONG =>
        provider.execute(
          command,
          Right(DALProvider.readFirstLongValue),
          proj.key,
          c.rowTypeInfo,
          executeOptions.asEntitledOnlyIf(proj.entitledOnly))
      case x =>
        throw new RelationalUnsupportedException(s"Unsupported projector: $x")
    }
  }

  protected override def handleMethod(method: MethodElement): RelationElement = {
    super.handleMethod(method)
  }

  protected override def getReaderFunction(column: ColumnElement, returnType: TypeInfo[_]): Option[RelationElement] = {
    // we will always use compileAndExecute to avoid compilation
    None
  }

  protected override def handleDALHeapEntity(de: DALHeapEntityElement): RelationElement = {
    de.members.head match {
      case e: ColumnElement => getReaderFunction(e, de.rowTypeInfo).getOrElse(e)
    }
  }

  protected override def getFormattedQuery(proj: ProjectionElement): FormattedQuery = {
    // for regular DAL query, we do not need ComparisonRewriter
    translator.dialect.format(proj.select)
  }
}
