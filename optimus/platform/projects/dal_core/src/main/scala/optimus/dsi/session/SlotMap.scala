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
package optimus.dsi.session

final case class SlotMap(private[optimus] val map: Map[String, Int]) {
  def get(className: String) = map.getOrElse(className, 0)
  def ++(sm: SlotMap) = SlotMap(map ++ sm.map)
}

object SlotMap {
  val Empty = apply(Map())
  val Default = Empty
}

trait SlotMapProvider {
  def slotMap: SlotMap
}

trait SessionMetadataProvider extends SlotMapProvider {
  def classpathRegistered: Boolean
}
object SessionMetadataProvider {
  val InvalidProvider: SessionMetadataProvider = new SessionMetadataProvider {
    override def classpathRegistered: Boolean = throw new UnsupportedOperationException
    override def slotMap: SlotMap = throw new UnsupportedOperationException
  }
}

final case class SessionMetadataProviderImpl(slotMap: SlotMap, classpathRegistered: Boolean)
    extends SessionMetadataProvider
