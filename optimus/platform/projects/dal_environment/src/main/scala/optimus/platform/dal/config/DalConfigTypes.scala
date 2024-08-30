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

import com.typesafe.config.ConfigFactory

import java.util.regex.Pattern

final case class DalAppId(underlying: String) extends AnyVal {
  def isUnknown: Boolean = this == DalAppId.unknown
  override def toString: String = underlying
}
object DalAppId {
  val default = DalAppId("default")
  val noClientSession = DalAppId("noClientSession")
  val unknown = DalAppId("")
  implicit def orderingByUnderlying: Ordering[DalAppId] = Ordering.by(_.underlying)
}

final case class DalZoneId(underlying: String) extends AnyVal {
  def isUnknown: Boolean = this == DalZoneId.unknown
  override def toString: String = underlying
}
object DalZoneId {
  val unknown = DalZoneId("")
  val default = DalZoneId("DEFAULT_ZONE")
  private[optimus] val idle = DalZoneId("IDLE_ZONE")
  private[optimus] val dalMetrics = DalZoneId("DALMetrics")
  implicit def orderingByUnderlying: Ordering[DalZoneId] = Ordering.by(_.underlying)
}

final case class Host(underlying: String) extends AnyVal {
  override def toString: String = underlying
  def toStringNoSuffix(): String = underlying.stripSuffix(Host.msHostSuffix)
}
object Host {
  private val config = ConfigFactory.load("properties.conf")
  private final val msHostSuffix = config.getString("ms-host-suffix")
}

final case class Port(underlying: String) extends AnyVal {
  override def toString: String = underlying
}
object Port {
  def apply(underlying: Int): Port = Port(underlying.toString)
}

final case class UserId(underlying: String) extends AnyVal {
  override def toString: String = underlying
}
object UserId {
  private[optimus] val Unknown = apply("<unknown>")
}

final case class HostPort(host: Host, port: Port) {
  def hostport: String = s"${host.underlying}:${port.underlying}"
  def hostportWithoutSuffix: String = s"${host.toStringNoSuffix()}:${port.underlying}"
  override def toString(): String = s"HostPort(${host.underlying}, ${port.underlying})"
}

object HostPort {
  val NoHostPort = HostPort(Host(""), Port(""))
  val UnknownCli = HostPort(Host("unknownCli"), Port("unknownCli"))

  def apply(hostport: String): HostPort = {
    if (hostport.isEmpty || hostport.equals(":"))
      HostPort.NoHostPort
    else {
      val s: Seq[String] = hostport.split(":")
      if (s.size == 2)
        HostPort(Host(s(0)), Port(s(1)))
      else
        throw new RuntimeException(s"Invalid hostport: $hostport")
    }
  }
}

final case class ConnectedUser(userId: UserId, hostPort: HostPort) {
  override def toString = s"$user@${hostPort.hostport}"
  def user: String = userId.underlying
  def hostport: HostPort = hostPort
}
object ConnectedUser {
  private[optimus] val Unknown = ConnectedUser(UserId.Unknown, HostPort.NoHostPort)
}

final case class TreadmillApp(underlying: String) extends AnyVal {
  override def toString: String = underlying
}
final case class TreadmillInstanceId(underlying: String) extends AnyVal {
  override def toString: String = underlying
}

final case class ZkNodeName private (private val underlying: String) extends AnyVal {
  def nodeName: String = underlying
}
object ZkNodeName {
  // valid ZK node names must start with an alphabetic character and can only contain alphanumerics, underscore and
  // hyphen. They cannot be empty.
  private val validZkNodeName = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_\\-]*$")
  def fromString(str: String): ZkNodeName = {
    require(!(str eq null) && validZkNodeName.matcher(str).matches, s"Invalid ZK node name: $str")
    apply(str)
  }
}
