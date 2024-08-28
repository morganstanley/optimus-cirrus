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

import optimus.platform._
import optimus.platform.annotations.internal.EmbeddableMetaDataAnnotation
import optimus.platform.storable.Embeddable
import optimus.platform.storable.EmbeddableTraitCompanionBase
import optimus.platform.storable.HasDefaultUnpickleableValue

/**
 * [[JobConfiguration]] is the scenario-independent class that holds business-meaningful non-value affecting
 * parameters that can be attached to a [[ScenarioStack]].
 */
@EmbeddableMetaDataAnnotation(isTrait = true)
@embeddable trait JobConfiguration extends Embeddable
object JobConfiguration extends EmbeddableTraitCompanionBase with HasDefaultUnpickleableValue[JobConfiguration] {
  def defaultUnpickleableValue: JobConfiguration = MissingJobConfiguration
}

/** The default value when a [[JobConfiguration]] is missing. */
@EmbeddableMetaDataAnnotation(isObject = true)
@embeddable case object MissingJobConfiguration extends JobConfiguration
