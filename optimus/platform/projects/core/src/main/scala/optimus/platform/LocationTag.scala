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

import java.util.UUID

import msjava.base.util.uuid.MSUuid
import optimus.core.needsPlugin
import optimus.graph.Settings
import optimus.platform.annotations.withLocationTag

final case class LocationTag private (
    line: Int,
    column: Int,
    name: String,
    sourceFile: String,
    containing: AnyRef,
    derivedTags: List[LocationTag],
    extraParams: List[Any],
    nonRTUuid: UUID) {
  def withExtraParams(params: Any*): LocationTag = copy(extraParams = params.toList ::: extraParams)
  def withDerivedTag(): LocationTag = needsPlugin
  def withDerivedTag$tag(tag: LocationTag): LocationTag = copy(derivedTags = tag :: derivedTags)
}

object LocationTag {
  def apply(
      line: Int,
      column: Int,
      name: String,
      sourceFile: String,
      containing: AnyRef,
      derivedTags: List[LocationTag] = List(),
      extraParams: List[Any] = List()
  ): LocationTag = {
    val uuid = if (Settings.isLocationTagRT) null else MSUuid.generateJavaUUID()
    LocationTag(line, column, name, sourceFile, containing, derivedTags, extraParams, uuid)
  }

  @withLocationTag
  def getTag(): LocationTag = needsPlugin
  def getTag$LT(tag: LocationTag): LocationTag = tag

  def uniqueForTest(): LocationTag =
    LocationTag(-1, -1, null, null, null, null, null, nonRTUuid = MSUuid.generateJavaUUID())
}
