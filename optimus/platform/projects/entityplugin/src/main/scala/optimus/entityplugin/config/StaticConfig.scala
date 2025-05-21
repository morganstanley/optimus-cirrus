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
package optimus.entityplugin.config

import com.typesafe.config.ConfigFactory

import scala.annotation.nowarn
import scala.collection.immutable.Seq

object StaticConfig {

  // using getClass.getClassLoader here rather than the thread one as we the plugin classloader (not the OBT one!)
  private val config = ConfigFactory.parseResources(getClass.getClassLoader, "EntityPlugin.conf")

  // regex-ignore-start - cannot use jdk.CollectionConverters in compiler plugins!
  @nowarn("msg=10500 scala.collection.JavaConverters")
  def stringSet(key: String): Set[String] = {
    import scala.collection.JavaConverters._
    if (config.hasPath(key)) config.getStringList(key).asScala.toSet else Set.empty
  }
  // regex-ignore-end: cannot use jdk.CollectionConverters in compiler plugins!
}
