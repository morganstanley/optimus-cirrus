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
package optimus.stratosphere.config

import com.typesafe.config.Config
import optimus.scalacompat.collection._
import optimus.stratosphere.utils.RemoteUrl
import optimus.utils.MemSize

import java.nio.file.Path
import java.nio.file.Paths
import java.time.Instant
import java.time.{Duration => JDuration}
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

trait Extractor[+A] {
  def extract(config: Config, property: String): A
}

object Extractor extends LowerPriorityImplicits {
  implicit val stringExtractor: Extractor[String] = new Extractor[String] {
    override def extract(config: Config, property: String): String = config.getString(property)
  }
}

/** This trait is needed to make the [[String]] extractor the default one in case user does not provide any. */
trait LowerPriorityImplicits {

  implicit val pathExtractor: Extractor[Path] = new Extractor[Path] {
    override def extract(config: Config, property: String): Path = Paths.get(config.getString(property))
  }

  implicit val booleanExtractor: Extractor[Boolean] = new Extractor[Boolean] {
    override def extract(config: Config, property: String): Boolean = config.getBoolean(property)
  }

  implicit val intExtractor: Extractor[Int] = new Extractor[Int] {
    override def extract(config: Config, property: String): Int = config.getInt(property)
  }

  implicit val setExtractor: Extractor[Set[String]] = new Extractor[Set[String]] {
    override def extract(config: Config, property: String): Set[String] =
      config.getStringList(property).asScala.toSet
  }

  implicit val collectionExtractor: Extractor[Seq[String]] = new Extractor[Seq[String]] {
    override def extract(config: Config, property: String): Seq[String] =
      config.getStringList(property).asScala.toList
  }

  implicit val mapStringExtractor: Extractor[Map[String, String]] = new Extractor[Map[String, String]] {
    override def extract(config: Config, property: String): Map[String, String] =
      config.getObject(property).unwrapped().asScala.mapValuesNow(_.toString).toMap
  }

  implicit def mapExtractor[A: Extractor]: Extractor[Map[String, A]] = new Extractor[Map[String, A]] {
    override def extract(config: Config, property: String): Map[String, A] = {
      val configAtProperty = config.getConfig(property)
      configAtProperty
        .entrySet()
        .asScala
        .map(entry => entry.getKey -> implicitly[Extractor[A]].extract(configAtProperty, entry.getKey))
        .toMap
    }
  }

  implicit val configExtractor: Extractor[Config] = new Extractor[Config] {
    override def extract(config: Config, property: String): Config =
      config.getConfig(property)
  }

  implicit val configListExtractor: Extractor[Seq[Config]] = new Extractor[Seq[Config]] {
    override def extract(config: Config, property: String): Seq[Config] =
      config.getConfigList(property).asScala.toList
  }

  implicit val pathListExtractor: Extractor[Seq[Path]] = new Extractor[Seq[Path]] {
    override def extract(config: Config, property: String): Seq[Path] =
      collectionExtractor.extract(config, property).map(it => Paths.get(it))
  }

  implicit def optionalExtractor[A: Extractor]: Extractor[Option[A]] = new Extractor[Option[A]] {
    override def extract(config: Config, property: String): Option[A] =
      if (config.hasPath(property)) Some(implicitly[Extractor[A]].extract(config, property)) else None
  }

  implicit def seqMapExtractor: Extractor[Seq[Map[String, String]]] = new Extractor[Seq[Map[String, String]]] {
    override def extract(config: Config, property: String): Seq[Map[String, String]] =
      configListExtractor
        .extract(config, property)
        .map(_.entrySet().asScala.map(e => e.getKey -> e.getValue.unwrapped().toString).toMap)
  }

  implicit def finiteDurationExtractor: Extractor[FiniteDuration] = new Extractor[FiniteDuration] {
    override def extract(config: Config, property: String): FiniteDuration =
      Duration.fromNanos(config.getDuration(property).toNanos)
  }

  implicit def javaDurationExtractor: Extractor[JDuration] = new Extractor[JDuration] {
    override def extract(config: Config, property: String): JDuration = config.getDuration(property)
  }

  implicit def memSizeExtractor: Extractor[MemSize] = new Extractor[MemSize] {
    override def extract(config: Config, property: String): MemSize = MemSize.of(config.getString(property))
  }

  implicit def regexExtractor: Extractor[Regex] = new Extractor[Regex] {
    override def extract(config: Config, property: String): Regex = config.getString(property).r
  }

  implicit def instantExtractor: Extractor[Instant] = new Extractor[Instant] {
    override def extract(config: Config, property: String): Instant = Instant.ofEpochMilli(config.getLong(property))
  }

  implicit def remoteUrlExtractor: Extractor[RemoteUrl] = (config: Config, property: String) =>
    RemoteUrl(config.getString(property))

}
