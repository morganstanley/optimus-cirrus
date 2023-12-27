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
package optimus.buildtool.builders

import optimus.buildtool.config.CppBuildConfiguration
import optimus.buildtool.config.CppToolchain
import optimus.buildtool.scope.ScopedCompilation

object ScopeFilter {
  def apply(s: String): ScopeFilter = s.toLowerCase match {
    case "cpp"  => CppScopeFilter
    case "none" => NoFilter
  }
}

trait ScopeFilter {
  def apply(scope: ScopedCompilation): Boolean
}

object NoFilter extends ScopeFilter {
  override def apply(scope: ScopedCompilation): Boolean = true
}

object CppScopeFilter extends ScopeFilter {
  override def apply(scope: ScopedCompilation): Boolean =
    scope.config.cppConfigs.exists(cfg => isDefined(cfg.release) || isDefined(cfg.debug))

  private def isDefined(cfg: Option[CppBuildConfiguration]): Boolean =
    cfg.exists(_.toolchain != CppToolchain.NoToolchain)

}
