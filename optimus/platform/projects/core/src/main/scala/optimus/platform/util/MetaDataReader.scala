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
package optimus.platform.util

import java.io.ByteArrayOutputStream
import java.lang.reflect.Modifier
import msjava.base.io.IOUtils
import msjava.slf4jutils.scalalog.getLogger
import optimus.platform.annotations.internal.EntityMetaDataAnnotation
import optimus.platform.metadatas.internal.BaseMetaData
import optimus.platform.metadatas.internal.EmbeddableBaseMetaData
import optimus.platform.metadatas.internal.EntityBaseMetaData
import optimus.platform.metadatas.internal.EventBaseMetaData
import optimus.platform.metadatas.internal.MetaDataFiles
import optimus.platform.storable.Entity
import optimus.config.spray.json.JsonParser
import optimus.datatype.DatatypeUtil
import optimus.platform.metadatas.internal.MetaBaseMetaData
import optimus.platform.metadatas.internal.PIIDetails

import java.net.URL
import scala.collection.compat._
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object ClientEntityHierarchy {
  private[optimus] val hierarchy = new EntityHierarchyManager
}

object ClientEmbeddableHierarchy {
  private[optimus] val hierarchy = new EmbeddableHierarchyManager
}

trait EntityMetaDataManager {

  def getEntityInfo(clz: Class[_ <: Entity]): Option[EntityBaseMetaData]
  def getEntityInfo(className: String): Option[EntityBaseMetaData]

  def getSlot(clz: Class[_ <: Entity]): Option[Int] = getEntityInfo(clz) map toSlot

  def getSlot(className: String): Option[Int] = getEntityInfo(className) map toSlot

  protected def toSlot(info: EntityBaseMetaData): Int =
    if (info.isObject || !info.isStorable || info.isTrait) 0 else info.slotNumber

}

class ClassLoaderEntityMetaDataManager(classLoader: ClassLoader) extends EntityMetaDataManager {
  import scala.collection.concurrent._
  private val knownMappings = new TrieMap[String, Option[EntityBaseMetaData]]

  override def getEntityInfo(clz: Class[_ <: Entity]): Option[EntityBaseMetaData] =
    knownMappings.getOrElseUpdate(clz.getName, discoverInfo(clz))
  override def getEntityInfo(className: String): Option[EntityBaseMetaData] =
    knownMappings.getOrElseUpdate(className, discoverInfo(className))

  protected def discoverInfo(classname: String): Option[EntityBaseMetaData] =
    discoverInfo(Class.forName(classname, false, classLoader).asSubclass(classOf[Entity]))
  protected def discoverInfo(clz: Class[_ <: Entity]): Option[EntityBaseMetaData] = {
    Option(clz.getAnnotation(classOf[EntityMetaDataAnnotation])) map { annotation =>
      val parents = (clz.getInterfaces.toList ++ Option(clz.getSuperclass)) map (_.getName)

      val piiElementList: Seq[PIIDetails] = DatatypeUtil.extractPiiElementsList(clz.getDeclaredFields)

      EntityBaseMetaData(
        clz.getName,
        clz.getPackage.getName, //
        annotation.isStorable,
        Modifier.isAbstract(clz.getModifiers), //
        annotation.isObject,
        annotation.isTrait,
        annotation.slotNumber,
        annotation.explicitSlotNumber,
        parents,
        piiElements = piiElementList.toSeq
      )
    }
  }
}

object DefaultSlotDiscover {
  private val impl = new ClassLoaderEntityMetaDataManager(this.getClass.getClassLoader)
  // This is used for ClassEntityInfo to get slot number
  def getSlot(clz: Class[_ <: Entity]): Int = impl.getSlot(clz).get
}

trait ScanningEntityMetaDataManager extends EntityMetaDataManager {
  def discoverAllInfo: Map[String, BaseMetaData]
}

trait ResourceFinder {
  def getResources(name: String): Seq[URL]
}
class ClasspathResourceFinder(classLoader: ClassLoader) extends ResourceFinder {
  override def getResources(name: String): collection.Seq[URL] = classLoader.getResources(name).asScala.to(Seq)
}
object CurrentClasspathResourceFinder extends ClasspathResourceFinder(ClassLoaderHook.currentClassLoader)

class ExportedInfoScanner(
    resourceFinder: ResourceFinder = CurrentClasspathResourceFinder,
    classLoader: ClassLoader = ClassLoaderHook.currentClassLoader,
    metaDataFileName: String
) extends ScanningEntityMetaDataManager {
  import optimus.platform.metadatas.internal.MetaJsonProtocol._

  def this(classLoader: ClassLoader, metaDataFileName: String) =
    this(new ClasspathResourceFinder(classLoader), classLoader, metaDataFileName)

  private val log = getLogger(this)

  lazy val cachedInfo: Map[String, BaseMetaData] =
    baseMetadataPerJar.values.flatMap { metaData =>
      metaData.map(entity => entity.fullClassName -> entity)
    }.toMap

  lazy val cachedInfoByJar: Map[String, Seq[String]] =
    baseMetadataPerJar.map { case (jar, metaData) =>
      jar -> metaData.map(entity => entity.fullClassName)
    }

  private lazy val baseMetadataPerJar: Map[String, Seq[BaseMetaData]] = {
    val resources = resourceFinder.getResources(metaDataFileName)
    resources.map { resource =>
      val is = resource.openStream()
      val jarMetaData =
        try {
          val bufferSize = 65536 // 64 KB
          val result = new ByteArrayOutputStream(bufferSize)
          IOUtils.copy(is, result, bufferSize)
          result.flush()
          result.close()
          val bytes = result.toByteArray
          val json = JsonParser(bytes)
          val fileName = resource.getFile
          val metaData: Seq[BaseMetaData] =
            if (
              fileName.contains(MetaDataFiles.entityMetaDataFileName) ||
              fileName.contains(MetaDataFiles.storedEntityMetaDataFileName)
            ) {
              json.convertTo[List[EntityBaseMetaData]]
            } else if (fileName.contains(MetaDataFiles.embeddableMetaDataFileName)) {
              json.convertTo[List[EmbeddableBaseMetaData]]
            } else if (fileName.contains(MetaDataFiles.eventMetaDataFileName)) {
              json.convertTo[List[EventBaseMetaData]]
            } else if (fileName.contains(MetaDataFiles.metaSquaredDataFileName)) {
              json.convertTo[List[MetaBaseMetaData]]
            } else {
              throw new IllegalArgumentException(s"Invalid file name: $fileName")
            }
          metaData
        } catch {
          case NonFatal(e) =>
            log.error(s"MetaDataReader caught exception working on $resource", e)
            e.printStackTrace(System.out)
            throw e
        } finally {
          is.close()
        }
      resource.getPath -> jarMetaData
    }.toMap
  }

  override def discoverAllInfo: Map[String, BaseMetaData] = cachedInfo

  override def getEntityInfo(clz: Class[_ <: Entity]): Option[EntityBaseMetaData] = getEntityInfo(clz.getName)
  override def getEntityInfo(className: String): Option[EntityBaseMetaData] = cachedInfo.get(className) match {
    case Some(md: EntityBaseMetaData) => Some(md)
    case _                            => None
  }
}
