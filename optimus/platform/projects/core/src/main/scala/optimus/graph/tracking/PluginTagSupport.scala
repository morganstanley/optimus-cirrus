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
package optimus.graph.tracking

import optimus.platform._

private[tracking] trait PluginTagSupport {
  self: DependencyTracker =>

  final def setPluginTag[P](key: PluginTagKey[P], value: P): Unit = {
    addPluginTags(PluginTagKeyValue(key, value))
  }

  final def clearPluginTag[P](key: PluginTagKey[P]): Unit = {
    removePluginTags(key)
  }

  final def addPluginTags[P](pluginTagKeyValue: PluginTagKeyValue[P]): Unit = {
    nodeInputs.addInput(pluginTagKeyValue.key, pluginTagKeyValue.value)
    invalidatePluginTagMap()
  }

  final def removePluginTags(pluginTagKeyValue: PluginTagKey[_]): Unit = {
    nodeInputs.removeInput(pluginTagKeyValue)
    invalidatePluginTagMap()
  }

  private[tracking] def invalidatePluginTagMap(): Unit = {
    nodeInputs.invalidate()
    children foreach { ts =>
      ts.invalidatePluginTagMap()
    } // invalidation should propagate down to all children
  }
}
