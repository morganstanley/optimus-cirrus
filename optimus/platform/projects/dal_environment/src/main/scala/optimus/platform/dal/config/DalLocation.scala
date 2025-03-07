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
package optimus.platform.dal.config

import java.net.URI

import optimus.config.RuntimeEnvironmentEnum
import optimus.platform.{RuntimeEnvironmentKnownNames => KnownNames}
import optimus.platform.dal.DSIURIScheme
import optimus.platform.dsi.bitemporal.Context
import optimus.platform.dsi.bitemporal.DefaultContext
import optimus.platform.dsi.bitemporal.NamedContext

// Describes a DAL location to connect to. Currently there are two ways to connect to the DAL: using the name of an
// environment (e.g. "dev" -- see DalEnv), or using a URI (e.g. replica://devln?writer=dev -- see DalUri).
sealed trait DalLocation extends Any {
  // The context that this DAL location connects to
  def context: Context
}
object DalLocation {
  def apply(envConfigName: Option[DalEnv], dalURI: Option[String]): DalLocation = (envConfigName, dalURI) match {
    case (None, None)                       => throw new IllegalArgumentException("specify either env or uri")
    case (_: Some[DalEnv], _: Some[String]) => throw new IllegalArgumentException("specify either env or uri not both")
    case (Some(env), None)                  => env
    case (None, Some(uri))                  => DalUri(new URI(uri))
  }
}

final case class DalEnv(underlying: String) extends AnyVal with Env with DalLocation {
  def isUnknown: Boolean = this == DalEnv.unknown

  def components: (String, Option[String]) =
    underlying.split(":") match {
      case Array(mode)           => (mode, None)
      case Array(mode, instance) => (mode, Some(instance))
      case _                     => throw new IllegalArgumentException("Invalid environment name: " + underlying)
    }

  def mode: String = components match { case (m, _) => m }
  def instance: Option[String] = components match { case (_, i) => i }
  override def context: Context = DefaultContext
  override def toString: String = underlying

  def splitEnvAndRegion: (DalEnv, Option[String]) = {
    val (env, region) = components
    (DalEnv(env), region)
  }

  def atInstance(instance: String): DalEnv = DalEnv(s"$mode:$instance")
  def stripInstance(): DalEnv = DalEnv(mode)
}

object DalEnv {
  val unknown = DalEnv("")
  val mock = DalEnv(KnownNames.EnvMock)
  val none = DalEnv(KnownNames.EnvNone)
  val test = DalEnv(KnownNames.EnvTest)
  val prod = DalEnv("prod")

  val ci = DalEnv("ci")
  val release = DalEnv("release")
  val perf_test = DalEnv("perf_test")
  val reg_test = DalEnv("reg_test")
  val dev = DalEnv(KnownNames.EnvDev)
  val devdev = DalEnv("devdev")
  val obt_build_cache_dev = DalEnv("obt-build-cache-dev")
  val obt_build_cache_qa = DalEnv("obt-build-cache-qa")
  val obtdev = DalEnv("obtdev")
  val obtqa = DalEnv("obtqa")
  val qa = DalEnv("qa")
  val qai = DalEnv("qai")
  val bas = DalEnv("bas")
  val reg = DalEnv("reg")
  val reg2 = DalEnv("reg2")
  val snap = DalEnv("snap")
  val uat = DalEnv("uat")
  val praqa = DalEnv("praqa")
  val prod2 = DalEnv("prod2")

  // for integration tests
  val inttestPrefix = "inttest"

  implicit def orderingByUnderlying: Ordering[DalEnv] = Ordering.by(_.underlying)
  object Region {
    val prod2readOnly = "readonly"
    val prod2users = "users"
  }
}

final case class DalUri private (underlying: URI) extends AnyVal with DalLocation {
  override def context: Context = {
    val context = Context(DSIURIScheme.getQueryMap(underlying))
    context match {
      case NamedContext(name) =>
        require(
          name == DalUri.currentUser,
          s"Named context name $name is expected to be same as current user ${DalUri.currentUser}")
        context

      case _ =>
        context
    }
  }

  def isNonDefaultContext: Boolean =
    (underlying.getScheme == DSIURIScheme.BROKER || underlying.getScheme == DSIURIScheme.REPLICA) &&
      context != DefaultContext
}

object DalUri {
  def apply(uri: String): DalUri = apply(new URI(uri))
  def apply(uri: URI): DalUri = {
    // We do not allow "broker://<dalenv>" or "replica://<dalenv>" style URIs in non-dev envs.
    if (uriNotAllowed(uri))
      throw new IllegalArgumentException(s"specific uri value '$uri' not permitted for non-dev envs.")
    new DalUri(uri)
  }

  private def uriNotAllowed(uri: URI): Boolean = {
    if (uri.getScheme == DSIURIScheme.BROKER || uri.getScheme == DSIURIScheme.REPLICA) {
      val dalEnv = DALEnvs.getRuntimeEnvforDALEnv(DalEnv(uri.getAuthority))
      dalEnv != RuntimeEnvironmentEnum.dev && dalEnv != RuntimeEnvironmentEnum.test
    } else false
  }

  val currentUser: String = System.getProperty("user.name")
}

final case class DalProid(underlying: String) extends AnyVal
object DalProid {
  val undefined = DalProid("")
  val optdseng = DalProid("optdseng")
  val oddvbkrd = DalProid("oddvbkrd")
  val oqaibkrq = DalProid("oqaibkrq")
  val optsvdev = DalProid("optsvdev")
  val oprabkrq = DalProid("oprabkrq")
  val osnpbkrq = DalProid("osnpbkrq")
  val ouatbkrq = DalProid("ouatbkrq")
  val org2bkrq = DalProid("org2bkrq")
  val oregbkrq = DalProid("oregbkrq")
  val oqabkrq = DalProid("oqabkrq")
  val obasbkrq = DalProid("obasbkrq")
  val optsvprd = DalProid("optsvprd")
  val oedsbkrq = DalProid("oedsbkrq")

  // obt specific proid list
  val obtdskd = DalProid("obtdskd")
  val obtqskq = DalProid("obtqskq")

  // pmtools
  val optmonp = DalProid("optmonp")
  val optmonq = DalProid("optmonq")
  val optmond = DalProid("optmond")
}
