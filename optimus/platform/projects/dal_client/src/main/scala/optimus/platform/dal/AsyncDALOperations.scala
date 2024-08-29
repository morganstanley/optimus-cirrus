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
package optimus.platform.dal

import optimus.platform._
import optimus.platform.storable.SerializedEntity
import optimus.platform.dsi.bitemporal.{GetSlotsResult, ErrorResult, GetSlots}

/**
 * This trait is needed since EntityResolverReadImpl is an entity and so cannot have async methods
 */
// TODO (OPTIMUS-13031): Needs changing to an @transient @entity, but @async not supported on entities
trait AsyncDALOperations { this: DSIResolver with EntityResolverWriteImpl =>

  @async
  def getSchemaVersions(className: SerializedEntity.TypeRef): Set[Int] = {
    val cmd = GetSlots(className)
    val res = dsi.execute(cmd)
    val partition = partitionMap.partitionForType(className)
    res match {
      case Seq(err: ErrorResult, _*)  => throwErrorResult(err, partition)
      case Seq(GetSlotsResult(slots)) => slots
      case o => throw new GeneralDALException("Unexpected DSI result type " + o.getClass.getName)
    }
  }

}
