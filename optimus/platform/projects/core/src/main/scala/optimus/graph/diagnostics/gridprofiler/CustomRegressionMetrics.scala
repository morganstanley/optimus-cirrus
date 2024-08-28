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
package optimus.graph.diagnostics.gridprofiler

/*metrics displayed on Regression Page*/
object CustomRegressionMetrics {
  val dalAtNowCustomMetric = "dalAtNowCount"
  val dalPriqlCustomMetric = "dalPriqlCount"
  val dal1of1CustomMetric = "dal1of1Count"

  val dmcCacheComputeRatio = "dmcCacheComputeRatio"
  val dmcCacheMaxConsumption = "dmcCacheMaxConsumption"

  val customRegressionMetrics = "customRegressionMetrics"

  /*should contains all extended user defined metrics*/
  val metricsSet = Array(
    dalAtNowCustomMetric,
    dalPriqlCustomMetric,
    dal1of1CustomMetric,
    dmcCacheComputeRatio,
    dmcCacheMaxConsumption
  )
}
