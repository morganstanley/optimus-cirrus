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
package optimus.graph.diagnostics.pgo

import optimus.graph.Settings
import optimus.graph.diagnostics.PNodeTaskInfo

final case class AutoPGOThresholds(
    neverHitThreshold: Int = Settings.cacheConfigNeverHitThreshold,
    benefitThresholdMs: Double = Settings.cacheConfigBenefitThresholdMs,
    hitRatio: Double = Settings.cacheConfigHitRatio)

object ConfigWriterSettings {
  val default: ConfigWriterSettings = ConfigWriterSettings()

  def apply(
      modes: Seq[PGOMode] = Seq(DisableCache),
      thresholds: AutoPGOThresholds = AutoPGOThresholds(),
      includePreviouslyConfigured: Boolean = true,
      autoTrimCaches: Boolean = false): ConfigWriterSettings =
    new ConfigWriterSettings(modes.sortBy(-_.priority), thresholds, includePreviouslyConfigured, autoTrimCaches)
}

final case class ConfigWriterSettings private (
    modes: Seq[PGOMode],
    thresholds: AutoPGOThresholds,
    includePreviouslyConfigured: Boolean,
    autoTrimCaches: Boolean) {

  /** Return true if pnti has a cache policy configured through optconf AND we want to keep previous config */
  def optconfConfiguredPolicy(pnti: PNodeTaskInfo): Boolean =
    includePreviouslyConfigured && pnti.externallyConfiguredPolicy

  /** Return true if pnti has a cache configured through optconf AND we want to keep previous config */
  def optconfConfiguredCache(pnti: PNodeTaskInfo): Boolean =
    includePreviouslyConfigured && pnti.externallyConfiguredCache
}
