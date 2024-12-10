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
package optimus.platform.dsi.prc.session

import optimus.platform.dsi.bitemporal.Context
import optimus.platform.dsi.bitemporal.DefaultContextType
import optimus.platform.dsi.bitemporal.UniqueContextType
import optimus.platform.dsi.bitemporal.proto.CommandProtoSerialization
import optimus.platform.dsi.bitemporal.proto.ContextSerializer
import optimus.platform.dsi.bitemporal.proto.Dsi.ContextProto
import optimus.platform.dsi.bitemporal.proto.Prc.PrcClientSessionInfoProto
import optimus.platform.dsi.bitemporal.proto.ProtoSerializer

import scala.jdk.CollectionConverters._

private[optimus] object PrcClientSessionSerializer
    extends CommandProtoSerialization
    with ProtoSerializer[PrcClientSessionInfo, PrcClientSessionInfoProto] {
  override def serialize(value: PrcClientSessionInfo): PrcClientSessionInfoProto = ??? /* {

    val contextProto: ContextProto = value.context.contextType match {
      case DefaultContextType | UniqueContextType => ContextSerializer.serialize(value.context)
      case _ =>
        throw new IllegalArgumentException(s"Context ${value.context} is not supported.")
    }
    val builder = PrcClientSessionInfoProto.newBuilder
      .setReal(value.realId)
      .addAllEffectiveIds(value.roles.asJava)
      .setContext(contextProto)
      .setTxTime(toProto(value.establishmentTime))
    value.effectiveId.foreach(builder.setReadId)
    builder.build()
  } */

  override def deserialize(proto: PrcClientSessionInfoProto): PrcClientSessionInfo = ??? /* {
    val context: Context = ContextSerializer.deserialize(proto.getContext)
    val effectiveOpt = if (proto.hasReadId) Some(proto.getReadId) else None
    PrcClientSessionInfo(
      proto.getReal,
      effectiveOpt,
      proto.getEffectiveIdsList.asScala.toSet,
      fromProto(proto.getTxTime),
      context
    )
  } */
}
