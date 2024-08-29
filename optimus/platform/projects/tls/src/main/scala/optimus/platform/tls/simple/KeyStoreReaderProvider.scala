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
package optimus.platform.tls.simple

import com.google.common.base.Splitter

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

trait KeyStoreReaderProvider {
  def priority: Short
  def provide(configuration: KeyStoreReaderProvider.Configuration): Option[KeyStoreReader]
}
object KeyStoreReaderProvider {
  trait Configuration {
    def get(key: String): Option[AnyRef]
  }

  def resolve(configuration: KeyStoreReaderProvider.Configuration): Option[KeyStoreReader] =
    ServiceLoader
      .load(classOf[KeyStoreReaderProvider])
      .iterator()
      .asScala
      .toVector
      .sortBy(_.priority)
      .iterator
      .flatMap(_.provide(configuration))
      .find(_ => true)

  def usable(prefix: String, configuration: KeyStoreReaderProvider.Configuration): Boolean = {
    configuration.get("usage") match {
      case Some(usage: String) =>
        configuration
          .get(s"$prefix.usages")
          .collect {
            case usages: String =>
              Splitter.on(",").trimResults().omitEmptyStrings().split(usages).iterator().asScala.toSeq
            case usages => usages
          }
          .forall {
            case usages: Seq[_] => usages.contains(usage)
            case _              => false
          }
      case Some(_) => false
      case None    => true
    }
  }
}
