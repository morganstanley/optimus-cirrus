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
package optimus.buildtool.config

import optimus.platform._
import optimus.buildtool.utils.TypeClasses._

@entity object ScopeResolver {
  @node def resolveScopes(compilationScopeIds: Set[ScopeId], partialId: String): Set[ScopeId] = {
    val Seq(meta, bundle, module, tpe) = RelaxedIdString.parts(partialId)
    val scopes = tryResolveScopes(compilationScopeIds, partialId)

    scopes.getOrElse {
      throw new IllegalArgumentException(
        s"No scope(s) found matching partial ID '$partialId'" +
          s" (meta: ${str(meta)}, bundle: ${str(bundle)}, module: ${str(module)}, type: ${str(tpe)})")
    }
  }

  @node def tryResolveScopes(compilationScopeIds: Set[ScopeId], partialId: String): Option[Set[ScopeId]] = {
    val Seq(meta, bundle, module, tpe) = RelaxedIdString.parts(partialId)

    val scopes = compilationScopeIds
      .fieldFilter(_.meta, meta)
      .fieldFilter(_.bundle, bundle)
      .fieldFilter(_.module, module)
      .fieldFilter(_.tpe, tpe)

    if (scopes.isEmpty) None else Some(scopes)
  }

  private def str(field: Option[String]): String = field match {
    case Some(s) => s"'$s'"
    case None    => "<any>"
  }
}
