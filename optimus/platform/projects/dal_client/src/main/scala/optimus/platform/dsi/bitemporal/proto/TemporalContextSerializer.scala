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

import optimus.platform.TemporalContext
import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.temporalSurface._
import optimus.platform.temporalSurface.impl.FixedLeafTemporalSurfaceImpl

import scala.jdk.CollectionConverters._

/*
 * This is here because we need dal_client to be able to serialize
 * TemporalContexts, and we need dsi_versioningserver to be able to deserialize
 * them. The serialization logic is here because we need to put it on
 * FixedLeafTemporalSurfaceImpl in dal_client. It can't go in dal_core because we need
 * Namespace, which is defined in priql.
 * The deserialization logic is in platform where the classes to be instantiated
 * during deserialization are.
 */
trait TemporalContextSerialization extends ProtoSerializationTo {
  implicit val temporalContextSerializer: TemporalContextSerializer.type = TemporalContextSerializer
}

object TemporalContextSerializer
    extends BasicProtoSerialization
    with TemporalSurfaceMatcherSerialization
    with ProtoSerializerTo[TemporalContext, TemporalContextProto] {
  private[this] def helper(ts: TemporalSurface): TemporalContextProto = ??? /* {
    val isBranch: Boolean = !ts.isLeaf
    val isContext: Boolean = ts.isInstanceOf[TemporalContext]
    if (isBranch && ts.asInstanceOf[BranchTemporalSurface].scope != DataFreeTemporalSurfaceMatchers.allScope)
      throw new InvalidTemporalSurfaceScopeMatcherException(ts)
    val nonTickableTs: TemporalSurface = ts.frozen()

    val tcp: TemporalContextProto.Builder = TemporalContextProto.newBuilder
      .setIsBranch(isBranch)
      .setIsContext(isContext)
    if (isBranch) {
      tcp.addAllChildTemporalContexts(nonTickableTs.children.map(helper(_)).asJava)
    } else {
      tcp.setTemporalSurfaceMatcher(toProto(nonTickableTs.asInstanceOf[LeafTemporalSurface].matcher))
      if (isContext)
        tcp
          .setValidTime(toProto(nonTickableTs.asInstanceOf[TemporalContext].unsafeValidTime))
          .setTransactionTime(toProto(nonTickableTs.asInstanceOf[TemporalContext].unsafeTxTime))
      else
        tcp
          .setValidTime(toProto(nonTickableTs.asInstanceOf[FixedLeafTemporalSurfaceImpl].vt))
          .setTransactionTime(toProto(nonTickableTs.asInstanceOf[FixedLeafTemporalSurfaceImpl].tt))
    }
    if (nonTickableTs.tag.isDefined)
      tcp.setTag(
        nonTickableTs.tag.getOrElse(throw new IllegalStateException(s"tag should be defined for $nonTickableTs")))
    tcp.build
  } */

  // Use a helper function so that we can cast the TemporalContext to
  // TemporalSurface.
  override def serialize(tc: TemporalContext): TemporalContextProto = ??? /* {
    helper(tc.asInstanceOf[TemporalSurface])
  } */
}
