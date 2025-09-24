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
package optimus.observability

import optimus.graph.JobForwardingPluginTagKey
import optimus.platform._
import optimus.platform.annotations.internal.EmbeddableMetaDataAnnotation
import optimus.platform.storable.Embeddable
import optimus.platform.storable.EmbeddableTraitCompanionBase
import optimus.platform.storable.HasDefaultUnpickleableValue

/**
 * In an ideal world, we would collect all inputs to computations and we would let data speak for
 * itself by observing naturally occurring clusters. The next best thing is to expose a set of features
 * that are good proxies of the computation itself.
 *
 * [[StableCalcTags]] is both:
 *   - a way to expose a set of features for downstream analysis via ML / AI, and
 *   - a key to group computations (and related metrics) into discrete clusters.
 */
@EmbeddableMetaDataAnnotation(isTrait = true)
@embeddable trait StableCalcTags extends Embeddable {
  def tuples: Seq[(String, String)] = Seq.empty
}

object StableCalcTags
    extends EmbeddableTraitCompanionBase
    with JobForwardingPluginTagKey[StableCalcTags]
    with HasDefaultUnpickleableValue[StableCalcTags] {
  def defaultUnpickleableValue: StableCalcTags = MissingStableCalcTags
  val Missing: StableCalcTags = MissingStableCalcTags
}

/** The default value when a [[StableCalcTags]] is missing. */
@EmbeddableMetaDataAnnotation(isObject = true)
@embeddable private case object MissingStableCalcTags extends StableCalcTags
