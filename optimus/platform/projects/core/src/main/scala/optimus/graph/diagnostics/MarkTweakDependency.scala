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
package optimus.graph.diagnostics

import optimus.core.TPDMask
import scala.collection.mutable

object MarkTweakDependency {
  private var dependencyMap: mutable.WeakHashMap[PNodeTaskInfo, SelectionFlags.Value] = _
  def dependencyMapIsNull: Boolean = dependencyMap eq null

  def setTweakDependencyHighlight(selected: PNodeTaskInfo, pntis: Iterable[PNodeTaskInfo]): Unit = {
    if (selected.tweakDependencies == TPDMask.poison)
      setPoisonHighlight(selected, pntis)
    else
      setDependentHighlight(selected, pntis)
  }

  private def setPoisonHighlight(selected: PNodeTaskInfo, pntis: Iterable[PNodeTaskInfo]): Unit = {
    pntis.foreach(pnti => dependencyMap.put(pnti, SelectionFlags.NotFlagged))
    dependencyMap.put(selected, SelectionFlags.Poison)
  }

  private def setDependentHighlight(selected: PNodeTaskInfo, pntis: Iterable[PNodeTaskInfo]): Unit = {
    val tweakDependencies = selected.tweakDependencies
    pntis.foreach(pnti => {
      val mask = TPDMask.fromIndex(pnti.tweakID)
      if (tweakDependencies.intersects(mask))
        dependencyMap.put(pnti, SelectionFlags.Dependency)
      else
        dependencyMap.put(pnti, SelectionFlags.NotFlagged)
    })
    dependencyMap.put(selected, SelectionFlags.Dependent)
  }

  def getTweakDependencyHighlight(pnti: PNodeTaskInfo): Option[SelectionFlags.Value] = dependencyMap.get(pnti)
  def isMapInitialised: Boolean = dependencyMap ne null
  def intialiseMap(): Unit = {
    dependencyMap = mutable.WeakHashMap.empty
  }
}
