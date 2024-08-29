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
package optimus.platform.tls.pkcs12file

import optimus.platform.tls.simple.KeyStoreReader
import optimus.platform.tls.simple.KeyStoreReaderProvider

class Pkcs12FileKeyStoreReaderProvider extends KeyStoreReaderProvider {
  override def priority: Short = 16384
  override def provide(configuration: KeyStoreReaderProvider.Configuration): Option[KeyStoreReader] = {
    if (KeyStoreReaderProvider.usable("pkcs12file", configuration)) {
      (configuration.get("pkcs12file.path").collect({ case v => v.asInstanceOf[String] }).filter(_.nonEmpty) zip
        configuration.get("pkcs12file.askpass").collect({ case v => v.asInstanceOf[String] }).filter(_.nonEmpty)).map {
        case (path, askpass) =>
          Pkcs12FileKeyStoreReader(path, askpass)
      }.headOption
    } else None
  }
}
