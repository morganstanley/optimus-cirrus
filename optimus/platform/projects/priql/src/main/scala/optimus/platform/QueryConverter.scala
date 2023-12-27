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

import optimus.platform.relational.KeyPropagationPolicy
import optimus.platform.relational.csv.{AbstractCsvSource, CsvMultiRelation, CsvSource}
import optimus.platform.relational.excel.{ExcelMultiRelation, ExcelSource}
import optimus.platform.relational.inmemory.ScalaTypeMultiRelation
import optimus.platform.relational.namespace.{Namespace, NamespaceMultiRelation}
import optimus.platform.relational.tree.{MethodPosition, TypeInfo}
import optimus.platform.storable.Entity

/**
 * Higher-kind trait to convert from F[T] to Query[T]. Two implicit values are provided when F <:< Iterable and F <:<
 * Query (since it is invariant)
 */
trait QueryConverter[U, -F[_ <: U]] extends Serializable {

  /**
   * Converts the F[T] to Query[T]. The 'key', 'itemType' and 'p' are needed for this method since we could not capture
   * them at the context-bound place
   */
  def convert[T <: U](src: F[T], key: RelationKey[T], p: QueryProvider)(implicit
      itemType: TypeInfo[T],
      pos: MethodPosition): Query[T]
}

object QueryConverter extends LowPriorityConverters {
  implicit def iterableConverter[E]: QueryConverter[E, Iterable] = new IterableConverter[E]
  private class IterableConverter[E] extends QueryConverter[E, Iterable] {
    def convert[T <: E](src: Iterable[T], key: RelationKey[T], p: QueryProvider)(implicit
        itemType: TypeInfo[T],
        pos: MethodPosition): Query[T] = {
      IterableConverter.convert[T](src, key, p)(itemType, pos)
    }
  }
  object IterableConverter {
    def convert[T](src: Iterable[T], key: RelationKey[T], p: QueryProvider)(implicit
        itemType: TypeInfo[T],
        pos: MethodPosition): Query[T] = {
      val shouldExecuteDistinct = src match {
        case s: collection.Set[_] => false
        case s: java.util.Set[_]  => false
        case _                    => true
      }
      val iseq = src.toIndexedSeq
      val elem = ScalaTypeMultiRelation(iseq, key, pos, itemType, shouldExecuteDistinct, getKeyPolicy(p))
      p.createQuery(elem)
    }

    def convertUnique[T](src: Iterable[T], key: RelationKey[T], p: QueryProvider)(implicit
        itemType: TypeInfo[T],
        pos: MethodPosition): Query[T] = {
      val elem = ScalaTypeMultiRelation(src.toIndexedSeq, key, pos, itemType, false, getKeyPolicy(p))
      p.createQuery(elem)
    }
  }

  implicit def optionConverter[E]: QueryConverter[E, Option] = new OptionConverter[E]
  private class OptionConverter[E] extends QueryConverter[E, Option] {
    def convert[T <: E](src: Option[T], key: RelationKey[T], p: QueryProvider)(implicit
        itemType: TypeInfo[T],
        pos: MethodPosition): Query[T] = {
      val elem = ScalaTypeMultiRelation(
        iter = src.toIndexedSeq,
        key = key,
        typeInfo = itemType,
        shouldExecuteDistinct = false,
        pos = pos,
        keyPolicy = getKeyPolicy(p))
      p.createQuery(elem)
    }
  }

  implicit def selfConverter[E]: QueryConverter[E, Query] = new SelfConverter[E]
  private class SelfConverter[E] extends QueryConverter[E, Query] {
    // key will be ignored, provider won't change.
    def convert[T <: E](src: Query[T], key: RelationKey[T], p: QueryProvider)(implicit
        itemType: TypeInfo[T],
        pos: MethodPosition): Query[T] = {
      src
    }
  }

  implicit def csvEntityConverter[E <: Entity]: QueryConverter[E, CsvSource] = new CsvEntityConverter[E]
  private class CsvEntityConverter[E <: Entity] extends QueryConverter[E, CsvSource] {
    def convert[T <: E](src: CsvSource[T], key: RelationKey[T], p: QueryProvider)(implicit
        itemType: TypeInfo[T],
        pos: MethodPosition): Query[T] = {
      val elem = CsvMultiRelation(src, key, pos)
      p.createQuery(elem)
    }
  }

  implicit def excelConverter[E <: Entity]: QueryConverter[E, ExcelSource] = new ExcelConverter[E]
  private class ExcelConverter[E <: Entity] extends QueryConverter[E, ExcelSource] {
    def convert[T <: E](src: ExcelSource[T], key: RelationKey[T], p: QueryProvider)(implicit
        itemType: TypeInfo[T],
        pos: MethodPosition): Query[T] = {
      val elem = ExcelMultiRelation(src, key, pos)
      p.createQuery(elem)
    }
  }

  implicit def namespaceConverter[E]: QueryConverter[E, Namespace] = new NamespaceConverter[E]
  private class NamespaceConverter[E] extends QueryConverter[E, Namespace] {
    def convert[T <: E](src: Namespace[T], key: RelationKey[T], p: QueryProvider)(implicit
        itemType: TypeInfo[T],
        pos: MethodPosition): Query[T] = {
      val elem = NamespaceMultiRelation(src, key, pos)
      p.createQuery(elem)
    }
  }

  private def getKeyPolicy(p: QueryProvider): KeyPropagationPolicy = {
    p match {
      case x: KeyPropagationPolicy => x
      case _                       => KeyPropagationPolicy.NoKey
    }
  }
}

trait LowPriorityConverters {
  implicit def csvConverter[E]: QueryConverter[E, AbstractCsvSource] = new CsvConverter[E]
  private class CsvConverter[E] extends QueryConverter[E, AbstractCsvSource] {
    def convert[T <: E](src: AbstractCsvSource[T], key: RelationKey[T], p: QueryProvider)(implicit
        itemType: TypeInfo[T],
        pos: MethodPosition): Query[T] = {
      val elem = new CsvMultiRelation(src, itemType, key, pos)
      p.createQuery(elem)
    }
  }
}
