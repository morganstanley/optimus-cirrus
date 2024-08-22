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
package optimus.platform.temporalSurface

import optimus.graph.tracking.TrackingLeafTemporalSurface
import optimus.platform._
import optimus.platform.storable.EntityReference

import scala.concurrent.Future

// Loaded reflectively by RuntimeServiceLoader because it is needed in dal-client, which is a dependency of platform.
class TemporalSurfaceCacheFactoryImpl extends TemporalSurfaceCacheFactory {

  def cacheEntityReference(
      eref: EntityReference,
      clazz: Class[_],
      sourceTemporalitySurface: LeafTemporalSurface): Option[Future[SubscriptionID]] = {
    sourceTemporalitySurface match {
      case tracking: TrackingLeafTemporalSurface =>
        Some(
          // subscribes to eref is this leaf is a tracking leaf AND we are using it with a source that requires moving
          // time forward.
          tracking.implicitErefCacheIfTracking
            .map(_.asyncCacheIfAbsent(eref, clazz, partitionMapForNotification.partitionForType(clazz)))
            .getOrElse(Future.failed(new IllegalStateException("No implicit eref subscription manager added"))))
      case _ =>
        None
    }
  }
}
