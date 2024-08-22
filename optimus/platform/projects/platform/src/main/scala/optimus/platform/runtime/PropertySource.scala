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
package optimus.platform.runtime

import scala.jdk.CollectionConverters._

trait PropertySource {
  def getProperty(name: String): Option[String]
  def propertyNames: Seq[String]
}

class SystemPropertySource extends PropertySource {
  def getProperty(name: String) = Option(System.getProperty(name))
  def propertyNames: Seq[String] = {
    val it: Iterator[_] = System.getProperties.propertyNames.asScala
    it map { _.asInstanceOf[String] } toList
  }
}
