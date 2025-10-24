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
package optimus.platform

import optimus.platform.inputs.EngineForwarding.Behavior
import optimus.platform.inputs.NodeInputs.ScopedSINodeInput
import optimus.platform.inputs.ScopedSIEntry
import optimus.platform.inputs.EngineForwarding.Serialization
import optimus.platform.inputs.NodeInputMapValue
import optimus.platform.inputs.dist.GSFSections
import optimus.platform.inputs.dist.GSFSections.GSFSection
import optimus.platform.inputs.loaders.LoaderSource
import optimus.platform.inputs.registry.Source

import java.util.Collections
import java.util.Optional
import java.util.{List => JList}
import java.io.{Serializable => JSerializable}

/**
 * Supertype of plugin tags.
 *
 * @tparam T
 *   The payload type associated with this tag.
 */
sealed trait PluginTagKey[T] extends ScopedSINodeInput[T] {
  override def name: String = getClass.getSimpleName
  override final def description: String = "PluginTagKey: " + name
  override final def affectsNodeResult: Boolean = false
  override final def affectsExecutionProcessWide(): Boolean = false
  override final def sources: JList[Source[_, _, T]] = Collections.emptyList()
  override final def defaultValue: Optional[T] = Optional.empty() // is a method because Optional is not serializable
  override final def gsfSection: GSFSection[T, _] = GSFSections.none()
  override final def requiresRestart(): Boolean = false
  override final def combine(
      currSource: LoaderSource,
      currValue: T,
      newSource: LoaderSource,
      newValue: T): NodeInputMapValue[T] =
    new NodeInputMapValue[T](LoaderSource.CODE, newValue)
}

final case class PluginTagKeyValue[T](key: PluginTagKey[T], override val value: T) extends ScopedSIEntry[T](key, value)

/**
 * marker trait which flags that a plugin tag should be not be forwarded to engines
 *
 * @tparam T
 *   The payload type associated with this tag.
 */
trait NonForwardingPluginTagKey[T] extends PluginTagKey[T] {
  override final def serializationStyle: Serialization =
    throw new IllegalStateException("Should not look at the serialization behavior of something NEVER forwarding")
  override final def engineForwardingBehavior(): Behavior = Behavior.NEVER
}

trait ForwardingPluginTagKey[T] extends PluginTagKey[T] with JSerializable {
  override final def serializationStyle = Serialization.JAVA
  override final def engineForwardingBehavior(): Behavior = Behavior.ALWAYS
}

/**
 * marker trait which flags that a plugin tag may be forwarded when batching
 * @tparam T
 *   The payload type associated with this tag.
 */
trait BatchLevelPluginTagKey[T] { self: PluginTagKey[T] => }

/**
 * Currently used for XSFT when finding minimum scenario, to make sure pluginTags that could affect batching are not
 * dropped between proxies trying to reuse
 */
trait NotTransparentForCaching
