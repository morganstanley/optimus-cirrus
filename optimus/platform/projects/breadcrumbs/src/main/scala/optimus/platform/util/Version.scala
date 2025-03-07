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

package optimus.platform.util

import java.util.jar
import optimus.breadcrumbs.crumbs.Properties
import optimus.logging.LoggingInfo
import optimus.utils.PropertyUtils

import java.net.URL
import java.util.jar.Attributes
import scala.util.control.NonFatal

/*
 Are you interested in adding things to this object?

 *BEWARE THE STATIC INITIALIZATION!*

 This class is initialized statically (in <clinit>) in quite a few places in the codebase, which means that *ANY*
 exceptions thrown therein will produce an ExceptionInInitializer error and kill the JVM.

 Furthermore, static initialization is a minefield of poorly understood JVM race conditions and weird bugs! Avoid as
 much as possible the usage of vals and vars in this Object, because they will be instantiated during <clinit>. Bugs
 like these are way too much fun: hard to repro, intermittent, probably JVM version dependent. If you can avoid vals,
 please do!
 */

object Version extends Log {
  import Properties._

  lazy val version = fromManifest("Implementation-Version")
    .getOrElse(if (sys.env.contains("STRATO_INTELLIJ")) "local" else "unknown")
  lazy val commit = fromManifest("Build-Commit") getOrElse "unknown"
  lazy val obt = fromManifest("Buildtool-Version")
  lazy val strato = fromManifest("Stratosphere-Version")

  private def kv(key: Key[String], names: String*): Option[Elem[String]] =
    names.map(n => Option(System.getenv(n))).collectFirst { case Some(v) =>
      key -> v
    }

  lazy val jenkinsProperties: Elems = {
    kv(jenkinsTestCase, "TEST_CASE", "TEST_CASE_NAME") ::
      kv(jenkinsRegressionName, "TEST_NAME") ::
      kv(jenkinsAppName, "APP_NAME") ::
      kv(jenkinsModuleName, "MODULE_NAME") ::
      kv(jenkinsModuleGroupName, "MODULE_GROUP_NAME") ::
      kv(jenkinsBuildNumber, "REG_NUMBER") ::
      kv(jenkinsTestPhase, "TEST_PHASE") ::
      kv(prId, "PR_ID", "PULL_REQUEST_ID", "PR_NUMBER") ::
      kv(Properties.gsfControllerId, "GSF_CONTROLLER_ID") ::
      kv(Properties.kubeNodeName, "KUBE_NODE_NAME") ::
      kv(Properties.gsfEngineId, "GSF_ENGINE_ID") ::
      PropertyUtils.get("custom.id").map(customId -> _) ::
      Elems.Nil
  }

  lazy val shortProperties: Elems = {
    buildCommit -> commit ::
      installVersion -> version ::
      Properties.host -> LoggingInfo.getHost ::
      Properties.user -> LoggingInfo.getUser ::
      Properties.pid -> LoggingInfo.pid ::
      kv(Properties.idKvm, "ID_KVM", "ID_EXEC") ::
      Properties.osName -> LoggingInfo.os ::
      Elems.Nil
  }

  lazy val properties: Elems = {
    shortProperties ::: jenkinsProperties
  }

  lazy val verboseProperties: Elems = {
    Properties.javaOpts -> LoggingInfo.getJavaOpts() ::
      jenkinsProperties ::: shortProperties
  }

  private lazy val crumbsJar: URL = getClass.getProtectionDomain.getCodeSource.getLocation

  private lazy val manifestAttributes: Option[Attributes] = {
    val jarIn = new jar.JarInputStream(crumbsJar.openStream())
    try {
      Option(jarIn.getManifest).map(_.getMainAttributes)
    } catch { case NonFatal(_) => None }
    finally jarIn.close()
  }

  private def fromManifest(prop: String): Option[String] =
    manifestAttributes.flatMap(m => Option(m.getValue(prop)))

}
