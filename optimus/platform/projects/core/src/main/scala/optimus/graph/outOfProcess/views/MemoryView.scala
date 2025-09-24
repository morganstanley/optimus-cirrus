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

import optimus.graph.DiagnosticSettings
import optimus.graph.JMXConnection
import optimus.graph.cache.NCSupport

import scala.collection.mutable.ArrayBuffer

final case class MemoryView(
    countPara: Int,
    totalSize: Long,
    retainedSize: Long,
    sharedRetainedSize: Long,
    finalizer: String,
    name: String,
    reachability: String,
    retainedClassNames: Array[String],
    retainedClassSizes: Array[Int],
    sharedRetainedClassNames: Array[String],
    sharedClassSizes: Array[Int]
) {
  var size = totalSize
  var count = countPara
}

object MemoryViewHelper {
  def getMemory(precise: Boolean) = {
    val profile = NCSupport.updateProfile(null, precise, "~")
    if (DiagnosticSettings.outOfProcess) {
      JMXConnection.graph.getMemory(precise)
    } else {
      ArrayBuffer(profile.clsInfos.toVarArgsSeq: _*).map(x =>
        MemoryView(
          x.count,
          x.size,
          x.retainedSize,
          x.sharedRetainedSize,
          x.finalizer,
          x.name,
          x.reachability.toString,
          x.retainedClasses map (_.getName),
          x.retainedClassSizes,
          x.sharedRetainedClasses map (_.getName),
          x.sharedClassSizes
        ))
    }
  }
}
