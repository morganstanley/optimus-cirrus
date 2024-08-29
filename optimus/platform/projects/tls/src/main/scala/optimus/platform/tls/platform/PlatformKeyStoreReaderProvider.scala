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
package optimus.platform.tls.platform

import com.google.common.base.Splitter
import optimus.platform.tls.config.StaticConfig
import optimus.platform.tls.simple.KeyStoreReader
import optimus.platform.tls.simple.KeyStoreReaderProvider

import javax.naming.ldap.LdapName
import scala.jdk.CollectionConverters._

class PlatformKeyStoreReaderProvider extends KeyStoreReaderProvider {
  override def priority: Short = 24576
  override def provide(configuration: KeyStoreReaderProvider.Configuration): Option[KeyStoreReader] = {
    if (KeyStoreReaderProvider.usable("platform", configuration)) {
      val selectionByDefault =
        if (
          !configuration
            .get("platform.selector.defaults")
            .map {
              case value: String => value.toBoolean
              case value         => value
            }
            .contains(false)
        )
          Seq(
            StaticConfig.string("PROD_CERT_SUBJECT")
          )
        else Seq.empty

      val selector = Some(
        selectionByDefault ++
          configuration.get("platform.selector.principals").toSeq.flatMap {
            case keys: String =>
              Splitter.on(",").trimResults().omitEmptyStrings().split(keys).asScala.flatMap { key =>
                configuration.get(s"platform.selector.principal.$key").map(_.asInstanceOf[String])
              }
            case names: Seq[_] if names.forall(_.isInstanceOf[String]) =>
              names.asInstanceOf[Seq[String]]
            case _ => throw new IllegalArgumentException
          })
        .map(_.map(new LdapName(_)))
        .filter(_.nonEmpty)
        .map(_.toSet)

      Some(PlatformKeyStoreReader(selector))
    } else None
  }
}
