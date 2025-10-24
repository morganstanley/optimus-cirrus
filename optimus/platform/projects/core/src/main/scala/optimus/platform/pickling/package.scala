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
package optimus.platform

import scala.collection.BuildFrom
import scala.collection.mutable

package object pickling {
  implicit object pickledPropertiesBuildFrom extends BuildFrom[PickledProperties, (String, Any), PickledProperties] {
    override def fromSpecific(from: PickledProperties)(it: IterableOnce[(String, Any)]): PickledProperties = {
      newBuilder(from).addAll(it).result()
    }
    override def newBuilder(from: PickledProperties): mutable.Builder[(String, Any), PickledProperties] = {
      // We explicitly pass the Map.newBuilder as the underlying builder that PickledPropertiesBuilder will
      // use by-default will not preserve the order of the map we're iterating order
      // (see comments in PickledPropertiesBuilder)
      new PickledPropertiesBuilder(Map.newBuilder[String, Any])
    }
  }

  implicit def ppAsyncIterableMarker(c: PickledProperties): AsyncIterableMarker[(String, Any), PickledProperties] = {
    new AsyncIterableMarker(c).asInstanceOf[AsyncIterableMarker[(String, Any), PickledProperties]]
  }
}
