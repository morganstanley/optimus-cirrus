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
package optimus.platform.catalog

import optimus.platform.catalog
import optimus.platform.catalog.DataQualityDimension.DataQualityDimension

// docs-snippet:Controls
object DataQualityDimension extends Enumeration {
  type DataQualityDimension = Value
  val Completeness: catalog.DataQualityDimension.Value = Value(1)
  val Accuracy: catalog.DataQualityDimension.Value = Value(2)
  val CompletenessAccuracy: catalog.DataQualityDimension.Value = Value(3)
  val Timeliness: catalog.DataQualityDimension.Value = Value(4)
  val CompletenessTimeliness: catalog.DataQualityDimension.Value = Value(5)
  val AccuracyTimeliness: catalog.DataQualityDimension.Value = Value(6)
  val CompletenessAccuracyTimeliness: catalog.DataQualityDimension.Value = Value(7)
}
final case class DQControl(controlId: String, dimensions: DataQualityDimension*)
trait Controls {
  def dqControls: Seq[DQControl]
}
// docs-snippet:Controls

// docs-snippet:UpstreamDatasetsDefinition
trait UpstreamDatasets {
  def upstreamDatasets: Seq[UpstreamDataset]
}

trait BaseDatasetName

final case class UpstreamDataset(name: BaseDatasetName, producerEonId: Int, dqControls: Seq[DQControl]) extends Controls

// docs-snippet:UpstreamDatasetsDefinition
