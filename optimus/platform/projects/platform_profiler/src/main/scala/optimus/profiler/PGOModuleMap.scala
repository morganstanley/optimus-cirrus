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
package optimus.profiler

import optimus.platform.util.Log
import optimus.platform.util.json.DefaultJsonMapper
import spray.json._

object PGOModuleMap extends Log {
  private var groupToModule: Option[Map[String, Seq[String]]] = None

  // for tests
  def reset(): Unit = groupToModule = None

  def get(groupName: String): Option[Seq[String]] = groupToModule match {
    case None => log.error("PGOMap not initialised"); None
    case _    => groupToModule.get.get(groupName)
  }

  def fromContent(jsonContent: String): Unit =
    groupToModule = Some(PGOModuleParser.toMap[Seq[String]](jsonContent.parseJson.toString.trim))

  def partitionValidNames(groupNames: Seq[String]): (Seq[String], Seq[String]) =
    groupNames.partition(PGOModuleMap.get(_) ne None)
}

object PGOModuleParser {
  val mapper = DefaultJsonMapper.legacy

  def toMap[V](json: String)(implicit m: Manifest[V]): Map[String, V] = fromJson[Map[String, V]](json)

  def fromJson[T](json: String)(implicit m: Manifest[T]): T = {
    mapper.readValue[T](json)
  }
}
