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
package optimus.platform.dal.otelschema

object ReplicationInstrumentationSchema {
  val instrumentationLibraryName = "optimus.dal.instrumentation.replication"
  val attributePrefix = "optimus.dal.replication."
  val attributePrefixForMetrics = s"${attributePrefix}metrics."
  val spanCatchUpFetchOneGap = "catch_up/fetch_one_gap"
  val spanCatchUpProcessOneGap = "catch_up/process_one_gap"
  val eventProcessedOneGap = "catch_up/processed_one_gap"
  val eventGotOneGap = "catch_up/got_one_gap"
  val eventProcessedOneBatch = "catch_up/processed_one_batch"
  val spanDeserialization = "deserialize_transaction"
  // https://opentelemetry.io/docs/specs/otel/metrics/semantic_conventions/
  val metricsPrefix = "optimus.replication."
  val metricsPrefixForRealtimeWriter = "optimus.replicationrt."
}
