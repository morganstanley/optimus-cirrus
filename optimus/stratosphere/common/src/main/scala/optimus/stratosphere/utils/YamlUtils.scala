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
package optimus.stratosphere.utils

import java.util

import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.collection.immutable.SortedSet
import scala.jdk.CollectionConverters._

/**
 * A bunch of helper methods to work with Yaml files parsed to `java.util.Map[String, Any]`.
 */
trait YamlUtils {

  def scalaBoolean(map: Map[String, Any], name: String): Option[Boolean] = {
    map.get(name).map(_.asInstanceOf[Boolean])
  }

  def scalaMap(map: Any): Map[String, Any] = map match {
    case null => Map.empty
    case _    => map.asInstanceOf[util.Map[String, Any]].asScala.toMap
  }

  def scalaList(config: Map[String, Any], name: String): Seq[Map[String, Any]] = config.get(name) match {
    case Some(config) => scalaList(config)
    case None         => Seq.empty
  }

  def scalaListString(map: Map[String, Any], key: String): Seq[String] =
    map.get(key).map(_.asInstanceOf[java.util.List[String]].asScala.toIndexedSeq).getOrElse(Seq.empty)

  def scalaList(map: Any): Seq[Map[String, Any]] = map match {
    case null => Seq.empty
    case _    => map.asInstanceOf[util.List[util.Map[String, Any]]].asScala.toIndexedSeq.map(scalaMap)
  }

  def notNull(config: Map[String, Any], requiredNames: String*): Unit =
    notNull(config, requiredNames.toIndexedSeq, Seq())

  def notNull(config: Map[String, Any], requiredNames: Seq[String], optionalNames: Seq[String]): Unit = {
    for (name <- requiredNames) {
      assert(config.contains(name), s"Parameter $name is required! Found: ${config.keySet.mkString(", ")}.")
      assert(config(name) != null, s"Parameter $name cannot be null")
    }

    val allNames: Set[String] = (requiredNames ++ optionalNames).to(SortedSet)
    for (key <- config.keySet) {
      assert(allNames.contains(key), s"Parameter '$key' is not recognized, use one of: ${allNames.mkString(", ")}.")
    }
  }
}
