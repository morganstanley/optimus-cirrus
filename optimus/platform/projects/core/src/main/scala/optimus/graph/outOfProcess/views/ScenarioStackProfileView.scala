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
package optimus.graph.outOfProcess.views

import optimus.graph.diagnostics.NPTreeNode
import optimus.graph.diagnostics.GivenBlockProfile

import scala.collection.mutable.ArrayBuffer

final case class ScenarioStackProfileView(
    level: Int,
    name: String,
    created: Long,
    reUsed: Long,
    selfTime: Double,
    byNameTweak: String,
    source: String
) extends NPTreeNode {
  override def title: String = toString
  val children = new ArrayBuffer[ScenarioStackProfileView]()
  override def hasChildren: Boolean = children.nonEmpty
  override def getChildren: Iterable[ScenarioStackProfileView] = children
  override def toString: String = name + "(" + source + ")"
}

object ScenarioStackProfileViewHelper {

  def createScenarioStackProfileView(ss: Iterable[GivenBlockProfile]): Iterable[ScenarioStackProfileView] = {
    val lst = new ArrayBuffer[ScenarioStackProfileView]()

    def convertSStoView(ss: GivenBlockProfile) = {
      ScenarioStackProfileView(
        ss.level,
        ss.name(full = true),
        ss.getMisses,
        ss.getHits,
        ss.getSelfTimeScaled,
        ss.getByNameTweaks.toString,
        ss.source)
    }

    def dfs(node: GivenBlockProfile, nodeView: ScenarioStackProfileView): ScenarioStackProfileView = {
      if (!node.hasChildren)
        convertSStoView(node)
      else {
        val it = node.children.values().iterator()
        while (it.hasNext) {
          val child = it.next()
          val currentchildView = convertSStoView(child)
          nodeView.children.append(dfs(child, currentchildView))
        }
        nodeView
      }
    }

    for (su <- ss if ss ne null) {
      val ssview = convertSStoView(su)
      lst += dfs(su, ssview)
    }
    lst
  }

}
