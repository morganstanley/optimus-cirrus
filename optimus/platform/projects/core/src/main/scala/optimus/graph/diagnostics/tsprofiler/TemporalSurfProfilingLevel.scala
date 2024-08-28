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
package optimus.graph.diagnostics.tsprofiler

/*
  NONE  - default mode where we don't profile temporal surfaces
  --profile-temporal-surface none

  BASIC - basic mode where we profile everything and we aggregate profiling data for all TS children
  --profile-temporal-surface basic

  TOP_AGG - aggregated mode where we only keep track of hits and visit aggregated across all children (just top level metric)
    --profile-temporal-surface aggregate
 */
object TemporalSurfProfilingLevel extends Enumeration {
  type TemporalSurfProfilingLevel = Value
  val NONE, BASIC, AGGREGATE = Value
}
