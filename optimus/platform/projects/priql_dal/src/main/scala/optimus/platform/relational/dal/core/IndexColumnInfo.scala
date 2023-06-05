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
package optimus.platform.relational.dal.core

import optimus.entity.IndexInfo
import optimus.platform.pickling.Unpickler
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.data.tree.ColumnInfo
import optimus.platform.relational.data.tree.ColumnType
import optimus.platform.storable.EntityReference
import optimus.platform.storable.SerializedKey
import optimus.platform.storable.Storable

sealed trait IndexColumnInfo extends ColumnInfo {
  def unpickler: Option[Unpickler[_]] = None
  def indexed: Boolean
  def unique: Boolean
  def isCollection: Boolean
  def storableClass: Class[_ <: Storable]
  def toSerializedKey(value: Any): SerializedKey
}

object IndexColumnInfo {
  final class DefaultIndexColumnInfo(val index: IndexInfo[_ <: Storable, _]) extends IndexColumnInfo {
    val columnType =
      if (!index.unique) ColumnType.Index else if (index.indexed) ColumnType.UniqueIndex else ColumnType.Key
    def indexed = index.indexed
    def unique = index.unique
    def isCollection = index.isCollection
    def storableClass = index.storableClass
    def toSerializedKey(value: Any): SerializedKey = {
      index.asInstanceOf[IndexInfo[_, Any]].makeKey(value).toSerializedKey
    }
    override def toString: String = s"DefaultIndexColumnInfo(${storableClass.getSimpleName}:${index.name},U:${unique})"
  }

  final class EntityReferenceIndexColumnInfo(val index: IndexInfo[_ <: Storable, _]) extends IndexColumnInfo {
    require(
      index.propertyNames.size == 1,
      s"Invalid index with multiple properties: ${index.propertyNames.mkString(", ")}")

    val columnType =
      if (!index.unique) ColumnType.Index else if (index.indexed) ColumnType.UniqueIndex else ColumnType.Key
    def indexed = index.indexed
    def unique = index.unique
    def isCollection = index.isCollection
    def storableClass = index.storableClass
    def toSerializedKey(value: Any): SerializedKey = {
      val indexName = index.propertyNames.head
      val data = value match {
        case e: EntityReference       => Seq(indexName -> e)
        case Some(e: EntityReference) => Seq(indexName -> Seq(e))
        case None                     => Seq(indexName -> Seq())
        case _                        => throw new RelationalUnsupportedException(s"Unexpected value: $value")
      }
      SerializedKey(index.storableClass.getName, data, unique, indexed, index.queryByEref)
    }
    override def toString: String =
      s"EntityReferenceIndexColumnInfo(${storableClass.getSimpleName}:${index.name},U:${unique})"
  }

  def apply(index: IndexInfo[_ <: Storable, _]): IndexColumnInfo = new DefaultIndexColumnInfo(index)

  def asEntityRefColumnInfo(info: IndexColumnInfo): IndexColumnInfo = {
    info match {
      case i: DefaultIndexColumnInfo => new EntityReferenceIndexColumnInfo(i.index)
      case _ =>
        throw new RelationalUnsupportedException(s"Unexpected columnInfo type: ${info.getClass}")
    }
  }
}
