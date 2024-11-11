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
package optimus.graph.diagnostics.trace

/**
 * A mode that is like hotspots, but it has a timeline view to display in graph debugger.
 */
class OGEventsHotspotsWithTimelineObserver private[trace]
    extends OGEventsNewHotspotsObserver
    with OGEventsTimeLineObserverImpl {
  override def name: String = "hotspotsTimeline"
  override def title: String = "Hotspots (with timeline)"
  override def description: String = "Hotspots mode that includes timeline recording"
  override def includeInUI: Boolean = true
}
