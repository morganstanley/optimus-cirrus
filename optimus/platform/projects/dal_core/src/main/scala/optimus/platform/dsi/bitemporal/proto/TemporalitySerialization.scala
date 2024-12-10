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
package optimus.platform.dsi.bitemporal.proto

import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.dsi.bitemporal.DSIQueryTemporality
import java.time.Instant

import com.google.protobuf.MessageLiteOrBuilder
import msjava.slf4jutils.scalalog.Logger

trait DSIQueryTemporalitySerialization extends BasicProtoSerialization {
  protected def log: Logger

  def getTemporality(proto: MessageLiteOrBuilder {
    def hasBitempRange(): Boolean
    def getBitempRange(): BitempRangeProto

    def hasTxRange(): Boolean
    def getTxRange(): TimeIntervalProto

    def hasValidTime(): Boolean
    def getValidTime(): InstantProto

    def hasTxTime(): Boolean
    def getTxTime(): InstantProto

    def hasReadTxTime(): Boolean
    def getReadTxTime(): InstantProto
  }): DSIQueryTemporality = ??? /* {

    val temporality =
      if (proto.hasTxRange())
        DSIQueryTemporality.TxRange(fromProto(proto.getTxRange()))
      else if (proto.hasValidTime() && proto.hasTxTime())
        DSIQueryTemporality.At(fromProto(proto.getValidTime()), fromProto(proto.getTxTime()))
      else if (proto.hasValidTime()) {
        val readTT =
          if (proto.hasReadTxTime())
            fromProto(proto.getReadTxTime())
          else {
            log.error("Deserializing DSIQueryTemporality. ValidTime without readTxTime")
            patch.MilliInstant.now
          }
        DSIQueryTemporality.ValidTime(fromProto(proto.getValidTime()), readTT)
      } else if (proto.hasTxTime())
        DSIQueryTemporality.TxTime(fromProto(proto.getTxTime()))
      else if (proto.hasBitempRange()) {
        val inRange: Boolean = proto.getBitempRange().hasInRange() match {
          case true => proto.getBitempRange().getInRange()
          case _    => false
        }
        DSIQueryTemporality.BitempRange(
          fromProto(proto.getBitempRange().getVtRange()),
          fromProto(proto.getBitempRange().getTtRange()),
          inRange)
      } else {
        val readTT =
          if (proto.hasReadTxTime())
            fromProto(proto.getReadTxTime())
          else {
            log.error("Deserializing DSIQueryTemporality. All without readTxTime")
            patch.MilliInstant.now
          }
        DSIQueryTemporality.All(readTT)
      }
    temporality
  } */
}
