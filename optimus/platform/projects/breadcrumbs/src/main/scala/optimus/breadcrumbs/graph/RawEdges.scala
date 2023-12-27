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
package optimus.breadcrumbs.graph

import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.BreadcrumbsSendLimit.OnceBy
import optimus.breadcrumbs.BreadcrumbsSendLimit.OnceByCrumbEquality
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.CrumbNodeType
import optimus.breadcrumbs.crumbs.EdgeCrumb
import optimus.breadcrumbs.crumbs.EdgeType
import optimus.breadcrumbs.crumbs.NameCrumb

import java.lang

object RawEdges {
  val edgeCrumbsByDefault: Boolean = lang.Boolean.getBoolean("optimus.edge.crumbs")

  def ensureTracked(
      child: ChainedID,
      parent: => ChainedID,
      name: => String,
      tpe: CrumbNodeType.CrumbNodeType,
      edge: EdgeType.EdgeType,
      source: Crumb.Source = Crumb.OptimusSource): ChainedID = {
    if (
      Breadcrumbs.collecting && doSend(source)
      && (parent ne null) && (parent != child)
    ) {
      Breadcrumbs.info(OnceBy(child, parent), child, new EdgeCrumb(_, parent, edge, source))
      Breadcrumbs.info(OnceByCrumbEquality, child, new NameCrumb(_, source, name, tpe))
    }
    child
  }

  private def doSend(source: Crumb.Source): Boolean = edgeCrumbsByDefault || source != Crumb.OptimusSource
}
