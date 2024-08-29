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
package optimus.dal.silverking.client

import optimus.platform.dsi.bitemporal.ContextType
import optimus.platform.dsi.bitemporal.DefaultContextType
import optimus.platform.dsi.bitemporal.NamedContextType
import optimus.platform.dsi.bitemporal.SharedContextType
import optimus.platform.dsi.bitemporal.UniqueContextType

private[optimus] final class PrcNamespaceTemplate private (private val underlying: String) extends AnyVal {
  private[optimus] def namespaceForContextType(contextType: ContextType): String = {
    require(!contextType.id.contains(":"))
    val suffix = s"${contextType match {
        case DefaultContextType => ""
        case UniqueContextType  => s":${UniqueContextType.id}"
        case NamedContextType | SharedContextType =>
          throw new IllegalArgumentException(s"PRC does not support ${contextType.id}")
      }}"
    s"$underlying$suffix"
  }
}

private[optimus] object PrcNamespaceTemplate {
  def apply(template: String): PrcNamespaceTemplate = {
    require(!template.contains(":"), "namespace templates cannot contain delimiter string ':'")
    new PrcNamespaceTemplate(template)
  }

  private[optimus] def contextOfNamespace(nsId: String): ContextType = {
    val parts: Array[String] = nsId.split(":")
    parts match {
      case Array(_)                       => DefaultContextType // DefaultContext doesn't add a suffix
      case Array(_, UniqueContextType.id) => UniqueContextType
      case Array(_, NamedContextType.id) | Array(_, SharedContextType.id) =>
        throw new IllegalArgumentException(s"namespace ID $nsId resolves to unsupported context type")
      case _ => throw new IllegalArgumentException(s"Cannot derive context type from namespace id $nsId")
    }
  }
}
