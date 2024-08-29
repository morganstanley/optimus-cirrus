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
package optimus.platform.internal

import java.util.concurrent.ConcurrentHashMap

import msjava.slf4jutils.scalalog.getLogger
import optimus.entity.EntityInfoRegistry
import optimus.platform.dal.RuntimeProperties
import optimus.platform.EvaluationContext
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityMetadata
import optimus.platform.storable.EntityReference
import optimus.platform.storable.PersistentEntity

private[optimus] class ClassInfoBuilder {

  // Note we have concurrency as this is accessed from graph
  // we are using a raw CHM for performance, to avoid lambda generation and wrapping code
  // size should be limited to the number of entity classes
  private val metaCache = new ConcurrentHashMap[String, ClassInfo]
  private val typeIdToClassInfoCache = new ConcurrentHashMap[Int, ClassInfo]

  private val log = getLogger(this)

  def fromRef(eref: EntityReference): Option[ClassInfo] =
    eref.getTypeId.flatMap(id => Option(typeIdToClassInfoCache.get(id)))
  def fromMeta(metaData: EntityMetadata): ClassInfo =
    build(metaData.className, metaData.types, metaData.entityRef.getTypeId)
  def fromPersistentEntity(pe: PersistentEntity): ClassInfo = build(pe.className, pe.types, pe.entityRef.getTypeId)

  private def build(className: String, otherClassNames: Seq[String], typeIdOpt: Option[Int]) = {
    val res = metaCache.get(className)
    if (res ne null) {
      typeIdOpt.foreach(id => typeIdToClassInfoCache.putIfAbsent(id, res))
      res
    } else {
      val res =
        try ConcreteClassInfo(Class.forName(className).asSubclass(classOf[Entity]))
        catch {
          // match for either, but we need to reference t
          case t: Throwable if t.isInstanceOf[ClassNotFoundException] || t.isInstanceOf[LinkageError] =>
            upcastDomain match {
              case None =>
                val err = s"No class ${className} available and no upcasting domain specified"
                log.error(err, t)
                NoClassInfo(s"$err : $t")
              case domain: Some[String] => upcastClass(className, otherClassNames, domain)
            }
        }
      val updated = metaCache.putIfAbsent(className, res)
      val returnValue = if (updated ne null) updated else res
      typeIdOpt.foreach(id => typeIdToClassInfoCache.putIfAbsent(id, returnValue))
      returnValue
    }
  }
  private lazy val upcastDomain: Option[String] =
    EvaluationContext.env.config.runtimeConfig.getString(RuntimeProperties.DsiEntityUpcastingProperty)

  private def upcastClass(name: String, classNames: Seq[String], domain: Some[String]): ClassInfo = {
    val possibleUpcasts = classNames flatMap { possibleTarget =>
      try {
        val entityInfo = EntityInfoRegistry.getClassInfo(possibleTarget)
        if (entityInfo.upcastDomain == domain) Some(entityInfo) else None
      } catch {
        case _: ClassNotFoundException | _: LinkageError =>
          None
      }
    }
    possibleUpcasts find { entityInfo =>
      possibleUpcasts forall { _.runtimeClass.isAssignableFrom(entityInfo.runtimeClass) }
    } match {
      case None =>
        NoClassInfo(s"No upcast targets found for domain $domain in $possibleUpcasts for original class $name")
      case Some(upcast) => ConcreteClassInfo(upcast.runtimeClass)
    }
  }
}
