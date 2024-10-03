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
package optimus.config.scoped.json

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import optimus.config.scoped.ScopedConfiguration
import optimus.config.scoped.ScopedSchedulerPlugin
import optimus.graph.NodeTaskInfo
import optimus.graph.PropertyInfo

import java.io.File
import java.io.InputStream
import java.util.{Map => JMap}

final case class JsonScopedConfiguration private[json] (
    version: String = JsonScopedConfiguration.Version,
    @JsonProperty(value = "plugins") @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
    pluginConfigurations: JMap[String, ScopedSchedulerPlugin], // @JsonTypeInfo has no effect with Scala maps
    nodeConfigurations: Seq[JsonScopedConfiguration.NodeConfiguration]
) extends ScopedConfiguration {

  override def scopedPlugins: Map[NodeTaskInfo, ScopedSchedulerPlugin] = {
    val builder = Map.newBuilder[NodeTaskInfo, ScopedSchedulerPlugin]
    nodeConfigurations.foreach { nodeConfiguration =>
      val p = pluginConfigurations.get(nodeConfiguration.pluginName)
      if (p ne null) builder += nodeConfiguration.propertyInfo -> p
    }
    builder.result()
  }
}

private[scoped] object JsonScopedConfiguration {

  private[json] val Version = "1.0.0"

  private[json] val objectMapper: JsonMapper = JsonMapper
    .builder()
    .addModule(DefaultScalaModule)
    .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
    .build()

  def from(i: InputStream): JsonScopedConfiguration = objectMapper.readValue(i, classOf[JsonScopedConfiguration])
  def from(f: File): JsonScopedConfiguration = objectMapper.readValue(f, classOf[JsonScopedConfiguration])
  def from(a: Array[Byte]): JsonScopedConfiguration = objectMapper.readValue(a, classOf[JsonScopedConfiguration])
  def from(s: String): JsonScopedConfiguration = objectMapper.readValue(s, classOf[JsonScopedConfiguration])

  private[json] final case class NodeConfiguration(
      @JsonSerialize(using = classOf[PropertyInfoSerializer])
      @JsonDeserialize(using = classOf[PropertyInfoDeserializer])
      @JsonProperty(value = "nodeTaskInfo")
      propertyInfo: PropertyInfo[_],
      @JsonProperty(value = "plugin")
      pluginName: String
  )

}
