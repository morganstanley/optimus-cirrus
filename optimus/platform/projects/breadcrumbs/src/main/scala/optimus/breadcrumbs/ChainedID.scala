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
package optimus.breadcrumbs

import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger
import java.util.{ArrayList => JavaArrayList}
import java.util.{List => JavaList}
import java.util.{UUID => JUUID}
import msjava.base.util.uuid.{MSUuid => UUID}
import optimus.utils.PropertyUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * @param depth
 *   To support nested/scoped tracking
 * @param crumbLevel
 *   See BreadcrumbLevel (warn, info, debug ...)
 */
@SerialVersionUID(2017071401L)
final class ChainedID private[breadcrumbs] (val repr: String, val depth: Int, val crumbLevel: Int, val vertexId: String)
    extends Serializable {
  private[optimus] def this(repr: String, depth: Int, level: Int) =
    this(repr, depth, level, if (depth == 0) repr else (new UUID).toString)
  @transient private lazy val id = new AtomicInteger(0)
  private[optimus] def this(addr: InetAddress) = this(addr.getCanonicalHostName, 0, ChainedID.level)
  private[optimus] def this(j: JUUID) = this(j.toString, 0, ChainedID.level)
  def child: ChainedID = child(this.crumbLevel)
  def child(level: Int): ChainedID = new ChainedID(s"$repr#${id.incrementAndGet()}", depth + 1, level)
  def batch: ChainedID = new ChainedID(s"$repr#BATCH:${id.incrementAndGet()}", depth + 1, this.crumbLevel)

  def child(tag: ChainedID.TagTypes.TagTypes): ChainedID =
    new ChainedID(s"$repr#$tag:${id.incrementAndGet()}", depth + 1, crumbLevel)

  override def equals(that: Any): Boolean = that match {
    case that: ChainedID => that.vertexId == this.vertexId
    case _               => false
  }
  override def hashCode: Int = vertexId.hashCode
  override def toString: String = repr
  def prettyPrint: String = if (this.crumbLevel == ChainedID.level) repr else s"$repr (level: $crumbLevel)"
  def base: String = repr.replaceFirst("#[\\w#:]+$", "")

  private[optimus] def asList: JavaArrayList[String] = ChainedID.asList(this)

}

object ChainedID {
  final val ArrayRepVersion = 1

  object TagTypes extends Enumeration {
    type TagTypes = Value
    val BATCH, REQUEST = Value
  }

  final val prefix = PropertyUtils.get("breadcrumb.chainedid.prefix", "")
  private[breadcrumbs] val log: Logger = LoggerFactory.getLogger("ChainedID")
  private lazy val level: Int = BreadcrumbLevel.parse(PropertyUtils.get("breadcrumb.level", "DEFAULT")).value
  // Can't make this @deprecated, because of -Xfatal-warnings.  Can't make it deprecating, because that's defined in core.
  // (NB: there's no reason for that to be the case.)
  def empty: ChainedID = {
    if (log.isDebugEnabled)
      log.debug(
        "Thwarting attempt to create ChainedID with empty base",
        new IllegalArgumentException("Empty ChainedID"))
    else
      log.warn("Thwarting attempt to create ChainedID with empty base; enable DEBUG to locate malefactor.")
    root
  }
  def apply(repr: String, depth: Int = 0, level: Int = level) = new ChainedID(repr, depth, level)
  private[optimus] def parse(s: String) = new ChainedID(s, s.split("#").length - 1, 0)
  private[breadcrumbs] def parse(s: String, v: String) = new ChainedID(s, s.split("#").length - 1, 0, v)

  private[optimus] def asList(c: ChainedID): JavaArrayList[String] = {
    val result = new JavaArrayList[String](5)
    result.add(ChainedID.ArrayRepVersion.toString) // 0
    result.add(c.repr) // 1
    result.add(c.depth.toString) // 2
    result.add(c.crumbLevel.toString) // 3
    result.add(c.vertexId) // 4
    result
  }

  private[optimus] def fromList(a: JavaList[String]): ChainedID = {
    new ChainedID(a.get(1), a.get(2).toInt, a.get(3).toInt, a.get(4))
  }

  private[optimus] val root = {
    val cid: ChainedID = new ChainedID(prefix + (new UUID).toString, 0, level)
    // Logged at error to get around logging filters that get setup in various ways.  This is not an actual
    // error of course but we need to do it this way
    log.error(s"root chainedId: $cid (this is not an actual error!)")
    cid
  }
  def create(level: Int = ChainedID.level): ChainedID = root.child(level)
}
