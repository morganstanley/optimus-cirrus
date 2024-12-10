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

import optimus.platform.dsi.bitemporal.proto.Dsi.CommandAppNameTagProto
import scala.jdk.CollectionConverters._

final case class CommandAppNameTag(appNameTag: String, commandIndices: Seq[Int])

object CommandAppNameTagSerialization {
  final def toProto(commandAppNameTag: CommandAppNameTag): CommandAppNameTagProto = ??? /* {
    CommandAppNameTagProto.newBuilder
      .setTag(commandAppNameTag.appNameTag)
      .addAllCommandIndices(commandAppNameTag.commandIndices map (_.asInstanceOf[Integer]) asJava)
      .build
  } */
  final def fromProto(commandAppNameTagProto: CommandAppNameTagProto): CommandAppNameTag = ??? /* {
    val tag = commandAppNameTagProto.getTag()
    val indices = commandAppNameTagProto.getCommandIndicesList.asScala.map(_.asInstanceOf[Int])
    CommandAppNameTag(tag, indices)
  } */
}
