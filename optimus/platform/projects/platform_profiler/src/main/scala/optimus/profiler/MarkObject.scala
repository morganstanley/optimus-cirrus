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
package optimus.profiler

import java.util
import optimus.graph.NodeTask

object MarkObject {
  private val markedObjects: util.IdentityHashMap[NodeTask, String] = new util.IdentityHashMap()

  private[profiler] def addToMarkedObjects(task: NodeTask, label: String): Unit =
    markedObjects.put(task, label)

  private[profiler] def getLabelfromMarkedObjects(task: NodeTask): String =
    Option(markedObjects get task).getOrElse("")

  private[profiler] def getMarkedObjectByLabel(label: String): Option[NodeTask] = {
    val entrySet = markedObjects.entrySet().iterator()
    while (entrySet.hasNext) {
      val entry = entrySet.next()
      if (entry.getValue == label) {
        return Some(entry.getKey)
      }
    }
    None
  }

  private[profiler] def inMarkedObjects(task: NodeTask): Boolean =
    if (task != null) markedObjects.containsKey(task) else false

  private[profiler] def formatMarkedObject(task: NodeTask, taskName: String): String = {
    if (inMarkedObjects(task)) {
      s"""<html><b style="color:red;">[${getLabelfromMarkedObjects(task)}]</b> $taskName</html>"""
    } else taskName
  }

  private[profiler] def removeFromMarkedObjects(task: NodeTask): Unit =
    if (inMarkedObjects(task)) markedObjects remove task
}
