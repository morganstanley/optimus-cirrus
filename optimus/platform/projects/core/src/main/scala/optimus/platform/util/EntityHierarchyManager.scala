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

import optimus.platform.metadatas.internal.BaseMetaData
import optimus.platform.metadatas.internal.ClassMetaData
import optimus.platform.metadatas.internal.MetaDataFiles
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityImpl

abstract class BaseHierarchyManager(
    val resourceFinder: ResourceFinder,
    val classLoader: ClassLoader,
    metaDataFileName: String
) {
  import scala.collection.mutable

  final val metaData = {
    // mutable data used for performance to build result
    // the result is immutable
    // there is no multi-threaded access in this method
    // and the mutable data does not escape the method
    val md = new mutable.HashMap[String, BaseMetaData]
    new ExportedInfoScanner(resourceFinder, classLoader, metaDataFileName).discoverAllInfo.filterNot(e =>
      md.contains(e._1)) foreach { case (name, m) =>
      md(name) = m
    }
    ClassMetaData.buildMetaData(md.toMap)
  }
}

private trait ClassLoaderHook
object ClassLoaderHook {
  val currentClassLoader: ClassLoader = classOf[ClassLoaderHook].getClassLoader
}

class EntityHierarchyManager(
    resourceFinder: ResourceFinder = CurrentClasspathResourceFinder,
    classLoader: ClassLoader = ClassLoaderHook.currentClassLoader
) extends BaseHierarchyManager(resourceFinder, classLoader, MetaDataFiles.entityMetaDataFileName) {
  private[this] val entityImplName = classOf[EntityImpl].getName
  private[this] val entityName = classOf[Entity].getName
  private[optimus] def userDefined(emd: ClassMetaData) =
    emd.fullClassName != entityImplName && emd.fullClassName != entityName
}

class StoredHierarchyManager(
    resourceFinder: ResourceFinder = CurrentClasspathResourceFinder,
    classLoader: ClassLoader = ClassLoaderHook.currentClassLoader
) extends BaseHierarchyManager(resourceFinder, classLoader, MetaDataFiles.storedEntityMetaDataFileName)

class EmbeddableHierarchyManager(
    resourceFinder: ResourceFinder = CurrentClasspathResourceFinder,
    classLoader: ClassLoader = ClassLoaderHook.currentClassLoader
) extends BaseHierarchyManager(resourceFinder, classLoader, MetaDataFiles.embeddableMetaDataFileName)

class EventHierarchyManager(
    resourceFinder: ResourceFinder = CurrentClasspathResourceFinder,
    classLoader: ClassLoader = ClassLoaderHook.currentClassLoader
) extends BaseHierarchyManager(resourceFinder, classLoader, MetaDataFiles.eventMetaDataFileName)

class MetaHierarchyManager(
    resourceFinder: ResourceFinder = CurrentClasspathResourceFinder,
    classLoader: ClassLoader = ClassLoaderHook.currentClassLoader
) extends BaseHierarchyManager(resourceFinder, classLoader, MetaDataFiles.metaSquaredDataFileName)
