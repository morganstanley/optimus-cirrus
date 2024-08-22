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
package optimus.breadcrumbs.kafka

import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.Crumb.Headers
import optimus.breadcrumbs.kafka.KafkaTopicProperty.CrumbHintProperty
import optimus.breadcrumbs.kafka.KafkaTopicProperty.CrumbSourceProperty
import optimus.breadcrumbs.kafka.KafkaTopicProperty.CrumbTypeProperty

import java.util.{ Map => jMap }
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap

private[breadcrumbs] final case class KafkaTopic(topic: String) extends AnyVal

private[breadcrumbs] sealed trait KafkaTopicProperty {
  def name: String
}
private[breadcrumbs] object KafkaTopicProperty {
  def fromString(prop: String): KafkaTopicProperty = prop match {
    case CrumbHintProperty.name   => CrumbHintProperty
    case CrumbSourceProperty.name => CrumbSourceProperty
    case CrumbTypeProperty.name   => CrumbTypeProperty
    case DefaultProperty.name     => DefaultProperty
    case _                        => throw new IllegalArgumentException(s"Invalid KafkaTopicProperty: $prop")
  }
  case object CrumbHintProperty extends KafkaTopicProperty {
    override val name: String = "CrumbHint"
  }
  case object CrumbSourceProperty extends KafkaTopicProperty {
    override val name: String = "CrumbSource"
  }
  case object CrumbTypeProperty extends KafkaTopicProperty {
    override val name: String = "CrumbType"
  }
  case object DefaultProperty extends KafkaTopicProperty {
    override val name: String = "_default"
  }
}

private[breadcrumbs] final case class KafkaTopicMapping(property: KafkaTopicProperty, value: String, topic: KafkaTopic)
private[breadcrumbs] object KafkaTopicMapping {
  import KafkaTopicProperty.DefaultProperty

  val PropertyKey: String = "property"
  val ValueKey: String = "value"
  val TopicKey: String = "topic"

  def fromJava(jm: jMap[String, String]): KafkaTopicMapping = {
    if (jm.containsKey(DefaultProperty.name)) {
      KafkaTopicMapping(DefaultProperty, DefaultProperty.name, KafkaTopic(jm.get(DefaultProperty.name)))
    } else {
      KafkaTopicMapping(
        KafkaTopicProperty.fromString(jm.get(PropertyKey)),
        jm.get(ValueKey),
        KafkaTopic(jm.get(TopicKey)))
    }
  }
}

private[breadcrumbs] final case class BreadcrumbsKafkaTopicMappingKey(attr: String, prop: KafkaTopicProperty)

private[breadcrumbs] sealed trait BreadcrumbsKafkaTopicMapperT {
  def size: Int
  def topicForCrumb(c: Crumb): KafkaTopic
}
private[breadcrumbs] object BreadcrumbsKafkaTopicMapper {
  val DefaultKafkaTopic: KafkaTopic = KafkaTopic("crumbs")
  val MatchProperties: Seq[BreadcrumbsKafkaTopicMappingKey] = Seq(
    BreadcrumbsKafkaTopicMappingKey(Headers.Hints, CrumbHintProperty),
    BreadcrumbsKafkaTopicMappingKey(Headers.Source, CrumbSourceProperty),
    BreadcrumbsKafkaTopicMappingKey(Headers.Crumb, CrumbTypeProperty)
  )
  val MatchPropertiesNoHint: Seq[BreadcrumbsKafkaTopicMappingKey] = Seq(
    BreadcrumbsKafkaTopicMappingKey(Headers.Source, CrumbSourceProperty),
    BreadcrumbsKafkaTopicMappingKey(Headers.Crumb, CrumbTypeProperty)
  )
  private val log = getLogger(this.getClass)
}
private[breadcrumbs] class BreadcrumbsKafkaTopicMapper(topicsMap: Seq[KafkaTopicMapping])
    extends BreadcrumbsKafkaTopicMapperT {
  import BreadcrumbsKafkaTopicMapper._
  import KafkaTopicProperty._
  private[this] lazy val m: TrieMap[KafkaTopicProperty, TrieMap[String, KafkaTopic]] = {
    val mInner: TrieMap[KafkaTopicProperty, TrieMap[String, KafkaTopic]] =
      new TrieMap[KafkaTopicProperty, TrieMap[String, KafkaTopic]]()
    MatchProperties.foreach { key: BreadcrumbsKafkaTopicMappingKey =>
      if (!mInner.contains(key.prop)) {
        mInner.put(key.prop, new TrieMap[String, KafkaTopic]())
        topicsMap.foreach { ktp: KafkaTopicMapping =>
          if (key.prop == ktp.property) {
            mInner.getOrElseUpdate(key.prop, new TrieMap[String, KafkaTopic]()).put(ktp.value, ktp.topic)
          }
        }
      }
    }
    mInner
  }

  lazy val computedDefaultKafkaTopic: KafkaTopic = topicsMap
    .find(_.property == DefaultProperty)
    .map(_.topic)
    .getOrElse(DefaultKafkaTopic)

  override def size: Int = m.size
  override def topicForCrumb(c: Crumb): KafkaTopic = {
    @tailrec def outerHelper(matchProps: Seq[BreadcrumbsKafkaTopicMappingKey]): Option[KafkaTopic] = {
      matchProps.headOption.map(innerHelper) match {
        case Some(r) if r.isDefined  => r
        case _ if matchProps.isEmpty => None
        case _                       => outerHelper(matchProps.tail)
      }
    }
    def innerHelper(key: BreadcrumbsKafkaTopicMappingKey): Option[KafkaTopic] = {
      {
        if (c.hints.isEmpty) {
          c.header
        } else {
          c.header ++ Map(Headers.Hints -> c.hintString)
        }
      }.get(key.attr)
        .find { cProp: String =>
          m.get(key.prop).foldLeft[Boolean](false)((a, b) => a || b.contains(cProp))
        }
        .map(cProp => m.get(key.prop).get(cProp))
        .orElse({
          log.trace(s"Could not find prop ${key.prop} in ${m
              .map({ case (xKey: KafkaTopicProperty, xVal: TrieMap[String, KafkaTopic]) =>
                s"$xKey -> ${xVal.map({ case (yKey: String, yVal: KafkaTopic) => s"${yKey} -> ${yVal.topic}" }).mkString(", ")}"
              })
              .mkString(" | ")}")
          None
        })
    }
    if (c.hints.isEmpty) {
      outerHelper(MatchPropertiesNoHint).getOrElse(computedDefaultKafkaTopic)
    } else {
      outerHelper(MatchProperties).getOrElse(computedDefaultKafkaTopic)
    }
  }
}
