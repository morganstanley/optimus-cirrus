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
package optimus.platform.dsi.bitemporal.factory

import java.net.URI
import optimus.platform.dsi.bitemporal.DSI

//class URIDSIFactory(val factories: Map[String, DSIFactory[DSI, URI]])
//  extends DSIFactory[DSI, URI] {
//  case class UnsupportedDSIException(val uri: URI) extends Exception {
//    override def getMessage = "DSI factory not found for URI " + uri
//  }
//
//  def isDefinedAt(uri: URI): Boolean = factories.contains(uri.getScheme)
//
//  def createDSI(config: URI): DSI = {
//    factories.get(config.getScheme) match {
//      case Some(f) => f.createDSI(config)
//      case None => throw UnsupportedDSIException(config)
//    }
//  }
//
//  def configure(config: RuntimeConfiguration) {}
//}
//
final case class InvalidDSIURIException(val uri: URI, val reason: String) extends Exception {
  override def getMessage = "Invalid DSI URI " + uri + ": " + reason
}
