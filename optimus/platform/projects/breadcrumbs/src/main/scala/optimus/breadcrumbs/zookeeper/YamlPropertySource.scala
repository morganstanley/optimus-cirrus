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
package optimus.breadcrumbs.zookeeper

import java.util.{HashMap => JavaHashMap}
import java.util.{Map => JavaMap}

import com.netflix.config.WatchedUpdateListener
import msjava.zkapi.PropertySource
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MutableMap}

trait YamlPropertySource extends PropertySource {
  protected final val something = "placeholder string"
}

protected[breadcrumbs] object YamlContext {
  def apply(document: String) = new YamlContext(new Yaml().load(document).asInstanceOf[JavaMap[String, Object]])
  def apply(data: JavaMap[String, Object]) = new YamlContext(data)
}

protected[breadcrumbs] final class YamlContext private (protected[zookeeper] val data: JavaMap[String, Object])

protected[breadcrumbs] final class ReadOnlyYamlPropertySource(context: YamlContext, property: String = "")
    extends YamlPropertySource {
  private val writeNotSupportedMsg = "Unsupported write operation attempted on read-only property source"
  private val updatesNotSupportedMsg =
    "Unsupported update listener management operation attempted on read-only property source"

  private lazy val sourceObject = {
    if (property isEmpty)
      context.data asScala
    else
      property.split("/").foldLeft(context.data asScala) { (acc, element) =>
        acc get element getOrElse something match {
          case m: JavaMap[String, Object] @unchecked => m asScala
          case _                                     => MutableMap empty
        }
      }
  }

  def getProperty(key: String): Object = {
    sourceObject.get(key).getOrElse(null)
  }

  def setProperty(key: String, value: String): Unit = throw new RuntimeException(writeNotSupportedMsg)
  def setProperty(key: String, value: Object): Unit = throw new RuntimeException(writeNotSupportedMsg)
  def close(): Unit = {}
  def addUpdateListener(listener: WatchedUpdateListener): Unit = throw new RuntimeException(updatesNotSupportedMsg)
  def removeUpdateListener(listener: WatchedUpdateListener): Unit = throw new RuntimeException(updatesNotSupportedMsg)
  def getCurrentData(): JavaMap[String, Object] = new JavaHashMap(context.data)
}
