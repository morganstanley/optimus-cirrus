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

import optimus.graph.DiagnosticSettings
import optimus.platform.dsi.SupportedFeatures
import optimus.platform.embeddable

@embeddable sealed trait DalAsyncConfig extends Serializable {
  val async: Boolean
  val useOutOfWorkListener: Boolean
  val batchAccumulationWaitTime: Long
  val maxBatchSize: Int
  val maxBatchBuildTime: Long
  val maxBatchSizeForWrite: Int
}

@embeddable final case class DalAsyncBatchingConfig(
    async: Boolean,
    useOutOfWorkListener: Boolean,
    batchAccumulationWaitTime: Long,
    maxBatchSize: Int,
    maxBatchBuildTime: Long,
    maxBatchSizeForWrite: Int = 1)
    extends DalAsyncConfig

object DalAsyncBatchingConfig extends DalAsyncBatchingConfigProperties("dsi")

class DalAsyncBatchingConfigProperties(propKey: String) extends Serializable {
  def default: DalAsyncConfig = apply(SupportedFeatures.asyncClient)

  def apply(async: Boolean): DalAsyncBatchingConfig =
    DalAsyncBatchingConfig(
      async,
      useOutOfWorkListener,
      batchAccumulationWaitTime,
      maxBatchSize,
      maxBatchBuildTime,
      maxBatchSizeForWrites
    )

  val useOutOfWorkListener: Boolean = DiagnosticSettings.getBoolProperty(s"optimus.$propKey.useOutOfWorkListener", true)
  val batchAccumulationWaitTime: Long = java.lang.Long.getLong(s"optimus.$propKey.batchAccumulationWaitTime", 2L)
  val maxBatchSize: Int = Integer.getInteger(s"optimus.$propKey.maxBatchSize", 1000)
  val maxBatchBuildTime: Long = java.lang.Long.getLong(s"optimus.$propKey.maxBatchBuildTime", 100L)
  val maxBatchSizeForWrites: Int = Integer.getInteger(s"optimus.$propKey.maxBatchSizeForWrites", 1)
}
