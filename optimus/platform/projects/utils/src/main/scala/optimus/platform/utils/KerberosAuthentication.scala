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
package optimus.platform.utils

import javax.security.auth.login.AppConfigurationEntry
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag
// import msjava.kerberos.auth.MSKerberosConfiguration

import scala.jdk.CollectionConverters._

//TODO (OPTIMUS-48905): merge KerberosSupport with this implementation
trait KerberosAuthentication {
  def isKerberized: Boolean

  protected def setupKerberosCredentials(clientName: String): Unit = {
    setupKerberosCredentials(List(clientName))
  }

  protected def setupKerberosCredentials(clientNames: List[String] = List("KafkaClient", "Client")): Unit = {
    /* if (isKerberized) {
      MSKerberosConfiguration.setClientConfiguration()
      clientNames.foreach(clientName => registerAppConfigurationEntryFor(clientName))
    } */
  }

  private def registerAppConfigurationEntryFor(clientName: String): Unit = {
    /* MSKerberosConfiguration.registerAppConfigurationEntry(
      clientName,
      (_: String) =>
        Array(
          new AppConfigurationEntry(
            "com.sun.security.auth.module.Krb5LoginModule",
            LoginModuleControlFlag.REQUIRED,
            Map(
              "useKeyTab" -> "false",
              "storeKey" -> "false",
              "useTicketCache" -> "true",
              "principal" -> MSKerberosConfiguration.getUserPrincipal
            ).asJava
          )
        )
    ) */
  }
}
