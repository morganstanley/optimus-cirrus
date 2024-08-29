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
package optimus.platform.bitemporal

import optimus.platform._
import optimus.platform.storable.Entity

object TimelineHelper {

  /**
   * Goal to return "creation versions" in a sense that each Version of Entity (VersionedReference) has only a single
   * instance determined by "starting" vt-tt point. (one might consider as "version starting point").
   *
   * Similar logic defined here: optimus.platform.storable.EntityVersionHolder.
   */
  @scenarioIndependent @node def creationRectangles[E <: Entity](
      entityRects: Seq[EntityRectangle[E]]
  ): Seq[EntityRectangle[E]] = entityRects
    .groupBy(_.vref)
    .map { case (_, entries) =>
      entries.minBy(e => (e.ttFrom, e.vtFrom))
    }
    .toSeq
    .sortBy(e => (e.ttFrom, e.vtFrom))
}
