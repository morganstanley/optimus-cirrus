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
package optimus.platform.obt.openapi

import msjava.slf4jutils.scalalog.getLogger
import optimus.platform.OptimusApp
import optimus.platform.entersGraph

import java.net.HttpURLConnection
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

// Invokes a given rest app class in a simple mode (mock dal, no entitlements, etc), captures the OpenApi spec
// from the endpoint, then dumps it to a file. This should be used as part of a build task to ensure a correct
// spec is available for a given rest framework at build time, which can then be disted out.
object OpenApiSpecBuilder extends OptimusApp[OpenApiSpecBuilderCmdLine] {

  private val log = getLogger(this)

  // This corresponds to the surface we need from OptimusRestApp
  // in order to invoke the app, get the spec, and shut it down cleanly
  // changes here should be reflected in OptimusRestApp and vice-versa.
  // We use this type to cast the reflected object into a fake type
  // (n.b. we can't cast it to the real type due to classloading across jars)
  // and then invoke what we need without further reflective calls to the methods
  type ReflectedApp = {
    def invokeAndGetPort(args: Array[String]): Int
    def maybeShutdown(): Unit
  }

  def getRestApp(fqn: String): ReflectedApp = {
    log.debug("Reflecting rest app")
    val objectFqn = if (fqn.endsWith("$")) fqn else fqn + "$"
    val clazz = Class.forName(objectFqn)
    val constructor = clazz.getDeclaredConstructors.headOption.getOrElse(
      throw new IllegalStateException(s"No constructor found for $objectFqn!"))
    constructor.setAccessible(true)
    constructor.newInstance().asInstanceOf[ReflectedApp]
  }

  private val specArgs = Array("--env", "none", "--port", "0", "--disableCrumbs")
  def getSpecToFile(theApp: ReflectedApp, outputFile: Path, apiVersion: String): Path = {
    try {
      log.debug("Starting the app and retrieving its port...")
      val port = theApp.invokeAndGetPort(specArgs)
      writeSpecToFile(port, apiVersion, outputFile)
    } finally {
      log.debug("Stopping the app...")
      theApp.maybeShutdown()
    }
  }

  private def writeSpecToFile(port: Int, apiVersion: String, outputFile: Path): Path = {
    val address = s"http://localhost:$port/$apiVersion"
    val url = new URI(address).toURL
    log.debug(s"Fetching spec from $address...")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    try {
      val code = conn.getResponseCode
      if (code == 200) {
        val is = conn.getInputStream()
        try Files.copy(is, outputFile, StandardCopyOption.REPLACE_EXISTING)
        finally is.close()
        outputFile
      } else throw OpenApiSpecBuilderException(s"$address replied with code $code: ${conn.getResponseMessage}")
    } finally conn.disconnect()
  }

  @entersGraph override def run(): Unit = {
    val outputFile = Paths.get(cmdLine.outputFile)
    val restApp = getRestApp(cmdLine.target)
    getSpecToFile(restApp, outputFile, cmdLine.api)
  }
}

final case class OpenApiSpecBuilderException(msg: String) extends Exception(msg)
