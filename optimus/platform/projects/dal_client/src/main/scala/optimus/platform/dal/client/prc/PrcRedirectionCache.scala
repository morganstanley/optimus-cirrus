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
package optimus.platform.dal.client.prc

import com.google.common.collect.Sets
import msjava.slf4jutils.scalalog.getLogger
import optimus.platform.dal.prc.RedirectionReason._
import optimus.platform.dsi.bitemporal.DalPrcRedirectionResult
import optimus.platform.dsi.bitemporal.QueryTemporalityType.QueryTemporalityType
import optimus.platform.dsi.bitemporal.ReadOnlyCommandWithTemporality
import optimus.platform.dsi.bitemporal.proto.Dsi.ResultProto
// import optimus.platform.dsi.bitemporal.proto.Dsi.ResultProto.RedirectionReasonType
import optimus.platform.dsi.prc.cache.NonTemporalPrcKeyUserOpts

object PrcRedirectionCache {
  private val log = getLogger(PrcRedirectionCache)
}

class PrcRedirectionCache {
  import PrcRedirectionCache._
  import scala.jdk.CollectionConverters._

  private val rejectedCommandTypes = Sets.newConcurrentHashSet[Class[_]]()
  private val rejectedQueryTypes = Sets.newConcurrentHashSet[Class[_]]()
  private val rejectedTempTypes = Sets.newConcurrentHashSet[QueryTemporalityType]()

  private[dal] def getRejectedQueryTypes: Set[Class[_]] = rejectedQueryTypes.asScala.toSet
  private[dal] def getRejectedTempTypes: Set[QueryTemporalityType] = rejectedTempTypes.asScala.toSet
  private[dal] def getRejectedCommandTypes: Set[Class[_]] = rejectedCommandTypes.asScala.toSet

  def isEmpty: Boolean = {
    rejectedQueryTypes.isEmpty && rejectedTempTypes.isEmpty && rejectedCommandTypes.isEmpty
  }

  def update(userOpts: NonTemporalPrcKeyUserOpts, redirection: ResultProto): Unit = ??? /* {
    Option(redirection.getRedirectionReason).foreach {
      case RedirectionReasonType.UNSUPPORTED_COMMAND_TYPE => rejectedCommandTypes.add(userOpts.command.getClass)
      case RedirectionReasonType.UNSUPPORTED_QUERY_TYPE =>
        rejectedQueryTypes.add(userOpts.key.normalizedKeyComponent.getClass)
      case RedirectionReasonType.UNSUPPORTED_TEMPORALITY_TYPE =>
        userOpts.command match {
          case rc: ReadOnlyCommandWithTemporality => rejectedTempTypes.add(rc.temporality.tempType)
          case c =>
            log.error(s" Command $c does not match unsupported temporality type redirection")
        }
      case RedirectionReasonType.UNKNOWN_REDIRECTION_REASON | RedirectionReasonType.KEY_SERDE_FAILURE |
          RedirectionReasonType.READ_THROUGH_FAILURE | RedirectionReasonType.NO_READ_THROUGH |
          RedirectionReasonType.SESSION_LOADING_FAILURE | RedirectionReasonType.ENTITLEMENTS_REQUIRE_FURTHER_CHECKS |
          RedirectionReasonType.PAYLOAD_RETRIEVAL_FAILURE | RedirectionReasonType.PRC_NO_BROKERS_FAILURE |
          RedirectionReasonType.PRC_READ_THROUGH_TIMEOUT_FAILURE =>
        ()
    }
  } */

  def get(userOpts: NonTemporalPrcKeyUserOpts): Option[DalPrcRedirectionResult] = {
    // Check order now is same as server side: QueryType -> QueryTemporalityType
    if (rejectedCommandTypes.contains(userOpts.command.getClass)) {
      Some(DalPrcRedirectionResult(CommandTypeNotSupported))
    } else if (rejectedQueryTypes.contains(userOpts.key.normalizedKeyComponent.getClass)) {
      Some(DalPrcRedirectionResult(QueryTypeNotSupported))
    } else
      userOpts.command match {
        case rc: ReadOnlyCommandWithTemporality if (rejectedTempTypes.contains(rc.temporality.tempType)) =>
          Some(DalPrcRedirectionResult(TemporalityTypeNotSupported))
        case _ => None
      }
  }

  def clear(): Unit = {
    rejectedQueryTypes.clear()
    rejectedTempTypes.clear()
    rejectedCommandTypes.clear()
  }
}
