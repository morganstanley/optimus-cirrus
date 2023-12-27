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
package optimus.buildtool.app

import optimus.buildtool.artifacts.MessagesArtifact
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.buildtool.scope.CompilationNode
import optimus.buildtool.scope.ScopedCompilation
import optimus.platform._

import scala.collection.immutable.Seq

@entity trait ScopedCompilationFactory {
  @node def globalMessages: Seq[MessagesArtifact]
  @node def scopeIds: Set[ScopeId]
  @node def scopeConfigSource: ScopeConfigurationSource
  @node def scope(id: ScopeId): ScopedCompilation =
    lookupScope(id).getOrElse(throw new IllegalArgumentException(s"No scope found for $id"))
  @node def lookupScope(id: ScopeId): Option[ScopedCompilation]
  @node def mischiefScope(id: ScopeId): Boolean
  @node def freezeHash: Option[String]
}

@entity trait CompilationNodeFactory extends ScopedCompilationFactory {
  @node override def lookupScope(id: ScopeId): Option[CompilationNode]
}
