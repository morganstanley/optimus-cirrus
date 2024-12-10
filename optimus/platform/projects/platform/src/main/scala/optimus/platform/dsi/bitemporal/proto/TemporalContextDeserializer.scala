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

import java.time.Instant

import optimus.platform.TemporalContext
import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.temporalSurface.TemporalSurfaceDefinition.FixedBranchContext
import optimus.platform.temporalSurface.TemporalSurfaceDefinition.FixedBranchSurface
import optimus.platform.temporalSurface.TemporalSurfaceDefinition.FixedLeafContext
import optimus.platform.temporalSurface.TemporalSurfaceDefinition.FixedLeafSurface
import optimus.platform.temporalSurface._

import scala.jdk.CollectionConverters._

trait TemporalContextDeserialization extends ProtoSerializationFrom {
  implicit val temporalContextDeserializer: TemporalContextDeserializer.type = TemporalContextDeserializer
}

object TemporalContextDeserializer
    extends BasicProtoSerialization
    with TemporalSurfaceMatcherDeserialization
    with ProtoSerializerFrom[TemporalContext, TemporalContextProto] {
  private[this] def helper(proto: TemporalContextProto): TemporalSurface = ??? /* {
    val vt: Option[Instant] =
      if (proto.hasValidTime)
        Some(fromProto(proto.getValidTime))
      else
        None
    val tt: Option[Instant] =
      if (proto.hasTransactionTime)
        Some(fromProto(proto.getTransactionTime))
      else
        None
    val matcher: Option[TemporalSurfaceMatcher] =
      if (proto.hasTemporalSurfaceMatcher)
        Some(fromProto(proto.getTemporalSurfaceMatcher))
      else
        None
    val tag: Option[String] =
      if (proto.hasTag)
        Some(proto.getTag)
      else
        None
    if (proto.getIsBranch) {
      if (proto.hasTemporalSurfaceScopeMatcher)
        throw new InvalidTemporalSurfaceScopeMatcherProtoException(proto)
      if (proto.getIsContext)
        FixedBranchContext(
          TemporalSurfaceMatchers.allScope,
          proto.getChildTemporalContextsList.asScala.toList.map { child =>
            helper(child).asInstanceOf[FixedTemporalSurface]
          },
          tag)
      else
        FixedBranchSurface(
          TemporalSurfaceMatchers.allScope,
          proto.getChildTemporalContextsList.asScala.toList.map { child =>
            helper(child).asInstanceOf[FixedTemporalSurface]
          },
          tag)
    } else {
      if (proto.getIsContext)
        FixedLeafContext(
          matcher
            .getOrElse(throw new IllegalStateException("matcher should be defined."))
            .asInstanceOf[TemporalSurfaceMatcher],
          vt.getOrElse(throw new IllegalStateException("vt should be defined.")),
          tt.getOrElse(throw new IllegalStateException("tt should be defined.")),
          tag
        )
      else
        FixedLeafSurface(
          matcher
            .getOrElse(throw new IllegalStateException("matcher should be defined."))
            .asInstanceOf[TemporalSurfaceMatcher],
          vt.getOrElse(throw new IllegalStateException("vt should be defined.")),
          tt.getOrElse(throw new IllegalStateException("tt should be defined.")),
          tag
        )
    }
  } */

  // Use a helper function so that we can cast the outer TemporalSurface to a
  // TemporalContext, but keep the possibility of having TemporalSurfaces
  // internally.
  override def deserialize(proto: TemporalContextProto): TemporalContext = {
    helper(proto).asInstanceOf[TemporalContext]
  }
}
