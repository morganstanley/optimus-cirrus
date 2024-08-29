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

import java.time.Instant

import optimus.platform.dsi.accelerated.AcceleratedInfo
import optimus.platform.dsi.bitemporal.AccMetadataCommand
import optimus.platform.dsi.bitemporal.AccMetadataOp
import optimus.platform.storable.SerializedEntity

trait DALAccelerator {
  def resolver: ResolverImpl

  object accelerator {

    /**
     * create acc tables based on entity AcceleratedInfoE(clsName, slot) insert AccInfo in AccMetadataTable
     */
    private[optimus] def createTable(
        clsName: String,
        slot: Int,
        accTxTime: Instant,
        serialized: SerializedEntity): Unit = {
      // TODO (OPTIMUS-51356): change back to exact equality of classname after migration is complete
      require(serialized.className.endsWith(AcceleratedInfo.acceleratedInfoEName))
      resolver.executeAccMetadataCommand(
        AccMetadataCommand(clsName, slot, AccMetadataOp.Create, Some(accTxTime), Some(serialized)))
    }

    /**
     * update acc tables if they already exist in PG based on entity AcceleratedInfoE(clsName, slot) update AccInfo in
     * AccMetadataTable
     */
    private[optimus] def updateTable(
        clsName: String,
        slot: Int,
        accTxTime: Instant,
        serialized: SerializedEntity,
        forceCreateIndex: Boolean): Unit = {
      // TODO (OPTIMUS-51356): change back to exact equality of classname after migration is complete
      require(serialized.className.endsWith(AcceleratedInfo.acceleratedInfoEName))
      resolver.executeAccMetadataCommand(
        AccMetadataCommand(clsName, slot, AccMetadataOp.Update, Some(accTxTime), Some(serialized), forceCreateIndex))
    }

    /**
     * drop acc tables in PG corresponding to one entity AcceleratedInfoE whose key is (clsName, slot) drop AccInfo in
     * AccMetadataTable
     */
    private[optimus] def dropTable(
        clsName: String,
        slot: Int,
        accTxTime: Instant,
        serialized: SerializedEntity): Unit = {
      // TODO (OPTIMUS-51356): change back to exact equality of classname after migration is complete
      require(serialized.className.endsWith(AcceleratedInfo.acceleratedInfoEName))
      resolver.executeAccMetadataCommand(
        AccMetadataCommand(clsName, slot, AccMetadataOp.Drop, Some(accTxTime), Some(serialized)))
    }

    /*
     * advance the acc_tt in PG, this will make the accread broker to refresh its local accinfo cache
     * insert or update AccInfo in AccMetadataTable
     */
    private[optimus] def updateAccTxTime(
        clsName: String,
        slot: Int,
        accTxTime: Instant,
        serialized: SerializedEntity): Unit = {
      // TODO (OPTIMUS-51356): change back to exact equality of classname after migration is complete
      require(serialized.className.endsWith(AcceleratedInfo.acceleratedInfoEName))
      resolver.executeAccMetadataCommand(
        AccMetadataCommand(clsName, slot, AccMetadataOp.UpdateAccTxTime, Some(accTxTime), Some(serialized)))
    }
  }
}
