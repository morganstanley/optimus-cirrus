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

import optimus.graph.diagnostics.CounterGroupSimple
import optimus.graph.diagnostics.LongCounter
import optimus.graph.diagnostics.ObservationRegistry

object DalCounters {

  /**
   * number of requests made
   * via DALDSIExecutor.doExecuteQuery
   */
  val dalRequestCounter = new LongCounter("dalRequests")

  /**
   * number of entities read via
   * via DALDSIExecutor.doExecuteQuery
   * via SelResult / QueryResult (.value.size)
   */
  val dalResultCounter = new LongCounter("dalResults")

  /**
   * number of entities written to DAL
   * EntityResolverWriteImpl.executeAndRetryAppEvent results of type PutApplicationEventResult, including puts, invalidates and reverts
   * EntityResolverWriteImpl.executeSaveCommands count of PutResult
   */
  val dalWriteCounter = new LongCounter("dalWrites")

  ObservationRegistry.instance.registerCounterGroup(
    new CounterGroupSimple("dalCounters", Seq(dalRequestCounter, dalResultCounter, dalWriteCounter), Seq()))
}
