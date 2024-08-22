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
package optimus.profiler.ui

import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.profiler.ui.HotspotsTable._

import scala.collection.mutable.ArrayBuffer

object PNodeDiffTable {
  val columnsRequiredForOptconfPntis: ArrayBuffer[TableColumn[PNodeTaskInfo]] = ArrayBuffer(
    c_propertyNode,
    c_started,
    c_selfTime,
    c_cacheTime,
    c_cacheHit,
    c_cacheMiss
  )
}

final class PNodeDiffTable extends NPTable[PNodeTaskInfo] {
  setViewColumns()

  def setViewColumns(): Unit = setView(PNodeDiffTable.columnsRequiredForOptconfPntis)
}
