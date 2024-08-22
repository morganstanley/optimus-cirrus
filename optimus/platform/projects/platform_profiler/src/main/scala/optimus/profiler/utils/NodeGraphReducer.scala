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
package optimus.profiler.utils

import java.util.{Collection => JCollection}
import java.util.{HashMap => JHashMap}

import optimus.graph.OGTraceReader
import optimus.graph.diagnostics.PNodeTask
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.profiler.recipes.PNodeTaskInfoGrp

object NodeGraphReducer {
  def compress(reader: OGTraceReader, collapseNonInternal: Boolean): JCollection[PNodeTaskInfoGrp] = {
    new NodeGraphReducer(reader, collapseNonInternal).compressed()
  }
}

class NodeGraphReducer(reader: OGTraceReader, collapseNonInternal: Boolean) {
  private val ggrps = new JHashMap[Integer, PNodeTaskInfoGrp]

  private def globalCollapse(task: PNodeTask): Boolean = {
    if (collapseNonInternal) {
      val pnti = reader.getTaskInfo(task.infoId)
      (pnti ne null) && !pnti.isInternal
    } else (task.callers != null && task.callers.size > 1)
  }

  private def compressedGraphV1(parent: PNodeTaskInfoGrp, parentTask: PNodeTask): Unit = {
    val lgrps = parent.childrenMap
    if (parentTask.callees != null) {
      val it = parentTask.callees.iterator()
      while (it.hasNext) {
        val task = it.next()
        val globallyUnique = globalCollapse(task)
        var grp = lgrps.get(task.infoId)
        if (grp == null) {
          grp = new PNodeTaskInfoGrp(reader.getTaskInfo(task.infoId), 0)
          grp.incomingEdges = 1
          if (globallyUnique) {
            val ggrp = ggrps.get(task.infoId)
            if (ggrp == null) { ggrps.put(task.infoId, grp) }
            else { grp = ggrp }
          }
          lgrps.put(task.infoId, grp)
        } else {
          grp.incomingEdges += 1
        }
        if (task.callees != null) { compressedGraphV1(grp, task) }
      }
    }
  }

  def compressed(): JCollection[PNodeTaskInfoGrp] = {
    val rootGrp = new PNodeTaskInfoGrp(new PNodeTaskInfo(0, "sync", "root"), 0)
    compressedGraphV1(rootGrp, reader.getSyncRootTask)
    rootGrp.childrenMap.values()
  }
}
