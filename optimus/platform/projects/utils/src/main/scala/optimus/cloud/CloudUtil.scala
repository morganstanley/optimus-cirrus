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
package optimus.cloud

import java.nio.file.Files
import java.nio.file.Paths

import org.slf4j.LoggerFactory

object CloudUtil {
  private val log = LoggerFactory.getLogger(CloudUtil.getClass)

  val CloudMarkerFile = "/etc/cloud/cloud-marker"

  def isCloud(): Boolean = {
    try {
      val isCloud = Files.exists(Paths.get(CloudMarkerFile))
      log.info(s"Cloud check returns: $isCloud")
      isCloud
    } catch {
      case e: Exception =>
        log.info(s"Not a cloud environment.  $CloudMarkerFile file does not exists.", e)
        false
    }
  }

}
