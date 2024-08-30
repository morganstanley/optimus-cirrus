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
package optimus.platform.dal

import java.net.URI

object DSIURIScheme {
  val DSI = "dsi"
  val KTCP = "ktcp"
  val SKTCP = "sktcp"
  val TCP = "tcp"
  val STCP = "stcp"
  val SERVICE_DISCOVERY = "discovery"
  val REPLICA = "replica"
  val BROKER = "broker"

  // below ones are for ServerDSIs
  val MONGO = "mongo"
  val MEMORY = "memory"
  val POSTGRES = "postgres"

  def getQueryMap(uri: URI): Map[String, String] = {
    Option(uri.getQuery)
      .getOrElse("")
      .split("&")
      .map(_.split("="))
      .flatMap { kv =>
        if (kv.length == 2)
          Some(kv(0), kv(1))
        else
          None
      }
      .toMap
  }
}
