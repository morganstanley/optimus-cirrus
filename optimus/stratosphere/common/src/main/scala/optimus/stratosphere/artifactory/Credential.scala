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
package optimus.stratosphere.artifactory

import com.fasterxml.jackson.databind.ObjectMapper
import msjava.slf4jutils.scalalog.getLogger

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.Base64
import java.util.Properties

/** A basic authentication credential e.g. for Artifactory */
final case class Credential(host: String, user: String, password: String) {
  // reduce risk of logging passwords:
  override def toString: String = s"Credential($host, $user, ****)"

  lazy val toHttpBasicAuth: String = Credential.toHttpBasicAuth(user, password)
}

object Credential {
  private val log = getLogger(this.getClass)

  val empty: Credential = Credential("", "", "")

  def toHttpBasicAuth(user: String, password: String): String =
    s"Basic ${Base64.getEncoder.encodeToString(s"$user:$password".getBytes(StandardCharsets.UTF_8))}"

  def fromPropertiesFile(file: Path): Credential = {
    val props = new Properties
    val stream = Files.newInputStream(file)
    try props.load(stream)
    finally stream.close()
    fromProperties(props)
  }

  def fromProperties(props: Properties): Credential = Credential(
    host = props.getProperty("host"),
    user = props.getProperty("user"),
    password = props.getProperty("password"))

  def fromJfrogConfFile(file: Path): Credential = {
    if (!file.toFile.exists()) {
      log.warn(s"Credential file $file not found!")
      empty
    } else {
      val objMapper = new ObjectMapper()
      val fileString = Files.readString(file)
      val artifactoryStr = objMapper.readTree(fileString).get("artifactory").get(0)
      Credential(
        host = new URI(artifactoryStr.get("url").asText()).toURL.getHost,
        user = artifactoryStr.get("user").asText(),
        password = artifactoryStr.get("password").asText()
      )
    }
  }

}
