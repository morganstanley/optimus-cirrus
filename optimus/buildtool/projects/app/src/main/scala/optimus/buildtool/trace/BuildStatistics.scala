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
package optimus.buildtool.trace

import java.time.Duration
import java.time.Instant

import optimus.buildtool.config.ScopeId

import scala.collection.immutable.Seq

final case class BuildStatistics(
    start: Instant,
    end: Instant,
    durationByCategory: Map[String, Map[String, Long]],
    durationByScenario: Map[String, Map[String, Long]],
    durationCentilesByCategory: Map[String, Seq[(String, Long)]],
    failuresByCategory: Map[String, Int],
    errorsByCategory: Map[String, Long],
    warningsByCategory: Map[String, Long],
    durationByPhase: Map[String, Map[String, Long]],
    durationByScopeAndPhase: Map[ScopeId, Map[String, Map[String, Long]]],
    durationCentilesByPhase: Map[String, Seq[(String, Long)]],
    stats: Map[String, Long],
    statsByCategory: Map[String, Map[String, Long]],
    statsByScope: Map[ScopeId, Map[String, Long]],
    statsByScopeAndCategory: Map[ScopeId, Map[String, Map[String, Long]]]
) {
  lazy val wallTime: Duration = Duration.between(start, end)
}
