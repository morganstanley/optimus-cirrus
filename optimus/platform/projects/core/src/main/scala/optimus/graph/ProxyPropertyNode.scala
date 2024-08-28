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
package optimus.graph

import optimus.platform.util.PrettyStringBuilder
import optimus.platform.storable.Entity

/**
 * A Node implementation which proxies to an underlying Node. All implementations defer to the proxied Node and wait for
 * the results.
 *
 * @tparam R
 *   The result type.
 */
abstract class ProxyPropertyNode[R] extends PropertyNode[R] {
  final var srcNodeTemplate: PropertyNode[R] = _

  /** For prettyPrint */
  def name_suffix = ".."

  /** Proxies usually just merge the infos from the underlying node */
  override protected def initTrackingValue(): Unit = {}

  /**
   * The following set of functions are just to "pretend" this Proxy is almost the original node
   */
  override def tidyKey: PropertyNode[R] = srcNodeTemplate.tidyKey
  override def entity: Entity = srcNodeTemplate.entity
  override def propertyInfo: NodeTaskInfo = srcNodeTemplate.propertyInfo // Almost the same as the original node
  /** Execution should not see it as the original node */
  override def executionInfo: NodeTaskInfo = srcNodeTemplate.propertyInfo.proxyInfo
  override def args: Array[AnyRef] = srcNodeTemplate.args
  override def argsEquals(that: NodeKey[_]): Boolean = that match {
    case thatproxy: ProxyPropertyNode[_] => srcNodeTemplate.argsEquals(thatproxy.srcNodeTemplate)
    case _                               => srcNodeTemplate.argsEquals(that)
  }
  override def argsHash: Int = srcNodeTemplate.argsHash

  override def subProfile(): Entity = srcNodeTemplate.subProfile()
  override def writePrettyString(sb: PrettyStringBuilder): PrettyStringBuilder = {
    if (srcNodeTemplate eq null) sb ++= getClass.getName ++= "[template]"
    else {
      val prevShowNodeState = sb.showNodeState
      sb.showNodeState = false
      val showPrefix = executionInfo.rawName() != srcNodeTemplate.executionInfo.rawName()
      if (showPrefix) sb ++= executionInfo.rawName() ++= " "

      srcNodeTemplate.writePrettyString(sb)

      sb.showNodeState = prevShowNodeState
      if (!showPrefix)
        sb ++= name_suffix // No point to show prefix and suffix

      if (sb.showNodeState)
        sb ++= stateAsString()
      sb
    }
  }
}
