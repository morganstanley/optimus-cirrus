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
package optimus.platform.runtime

import java.time.Instant
import optimus.breadcrumbs.ChainedID
import optimus.config.RuntimeConfiguration

import scala.annotation.tailrec

object ConfigOverrider {
  @tailrec
  def getUnderlyingRuntimeConfiguration(config: RuntimeConfiguration): RuntimeConfiguration = {
    config match {
      case overrider: ConfigOverrider => getUnderlyingRuntimeConfiguration(overrider.config)
      case c                          => c
    }
  }
}

// TODO (OPTIMUS-16209): We could refactor this and encapsulate the override behavior
// within RuntimeConfiguration trait itself, rather than wrapping some underlying RuntimeConfiguration instance.
private[platform] class ConfigOverrider(
    val config: RuntimeConfiguration,
    val overrides: Map[String, Any]
) extends RuntimeConfiguration {
  def env: String = config.env
  def mode: String = config.mode
  def instance: Option[String] = config.instance

  override def specificPropertyMap: Map[String, String] =
    config.propertyMap ++
      overrides.flatMap {
        case (k, Some(v)) => Some(k -> s"$v")
        case (_, None)    => None
        case (k, v)       => Some(k -> s"$v")
      }

  // This is made lazy because in case of using ConfigOverrider on a RuntimeConfiguration which connects
  // to zookeeper it starts loading the config while resolving rootID. This creates problems in tests also.
  // Since the underlying config is itself lazy, it makes sense to keep this property lazy and load it on demand.
  override lazy val rootID = get(SharedRuntimeProperties.DsiIDProperty) match {
    case Some(a: ChainedID) => a
    case _                  => config.rootID
  }

  override def getProperties(baseName: String): Option[List[String]] = {
    overrides.get(baseName) match {
      case Some(a) if a.isInstanceOf[List[_]] => Some(a.asInstanceOf[List[String]])
      case _                                  => config.getProperties(baseName)
    }
  }

  override def getAllProperties(baseName: String): Option[Set[String]] = {
    overrides.get(baseName) match {
      case Some(a) if a.isInstanceOf[Set[_]]  => Some(a.asInstanceOf[Set[String]])
      case Some(a) if a.isInstanceOf[List[_]] => Some(a.asInstanceOf[List[String]].toSet)
      case _                                  => config.getAllProperties(baseName)
    }
  }

  override def getString(name: String): Option[String] = {
    overrides.get(name) match {
      case Some(a) if a.isInstanceOf[String] => Some(a.asInstanceOf[String])
      case _                                 => config.getString(name)
    }
  }

  override def getInt(name: String): Option[Int] = {
    overrides.get(name) match {
      case Some(a) if a.isInstanceOf[Int] => Some(a.asInstanceOf[Int])
      case _                              => config.getInt(name)
    }
  }

  override def getStringList(name: String): Option[List[String]] = {
    overrides.get(name) match {
      case Some(a) if a.isInstanceOf[List[_]] => Some(a.asInstanceOf[List[String]])
      case _                                  => config.getStringList(name)
    }
  }

  override def getByteVector(name: String): Option[Vector[Byte]] = {
    overrides.get(name) match {
      case Some(a: Vector[_]) => Some(a.asInstanceOf[Vector[Byte]])
      case _                  => config.getByteVector(name)
    }
  }

  override def getBoolean(name: String): Option[Boolean] = {
    overrides.get(name) match {
      case Some(a) if a.isInstanceOf[Boolean] => Some(a.asInstanceOf[Boolean])
      case _                                  => config.getBoolean(name)
    }
  }

  override def getInstant(name: String): Option[Instant] = {
    overrides.get(name) match {
      case Some(a) if a.isInstanceOf[Instant] => Some(a.asInstanceOf[Instant])
      case _                                  => config.getInstant(name)
    }
  }

  override def get(name: String): Option[Any] = {
    overrides.get(name) match {
      case Some(_) => overrides.get(name)
      case _       => config.get(name)
    }
  }

  override def getAttributes(name: String): Map[String, String] = {
    overrides.get(name) match {
      case Some(a) if a.isInstanceOf[Map[_, _]] => a.asInstanceOf[Map[String, String]]
      case _                                    => config.getAttributes(name)
    }
  }

  override def getStringListWithAttributes(name: String): List[(String, Map[String, String])] = {
    overrides.get(name) match {
      case Some(a) if a.isInstanceOf[List[_]] => a.asInstanceOf[List[(String, Map[String, String])]]
      case _                                  => config.getStringListWithAttributes(name)
    }
  }

  override def withOverride(name: String, value: Any) = new ConfigOverrider(config, overrides + ((name, value)))
  override def withOverrides(m: Map[String, Any]) = new ConfigOverrider(config, overrides ++ m)
}
