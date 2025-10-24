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
package optimus.platform.pickling

import optimus.graph.Settings

import scala.collection.mutable
import scala.util.hashing.MurmurHash3

// This trait is not intended to be referred to by any consuming class. We use it to capture the API's we need
// to implement in order to eventually have PickledProperties not have to extend Map[String, Any].
// Two implementations of this trait exists:
// trait PickledProperties extends Map[String, Any]
// trait PickledPropertiesIterable extends Iterable[String, Any]
// Our goal is to eventually rename PickledPropertiesIterable -> PickledProperties and delete the
// implementation that extends Map as well as this PickledPropertiesAPi trait but this approach allows us
// to remediate the codebase in a staged manner without having to address the whole code base in one
// giant Pull Request.
trait PickledPropertiesApi[R] {
  def isEmpty: Boolean
  def hashCode: Int
  def knownSize: Int
  def size: Int
  def contains(key: String): Boolean
  def keys: Iterable[String]
  def values: Iterable[Any]
  def iterator: Iterator[(String, Any)]
  def get(key: String): Option[Any]
  def apply(key: String): Any
  def mapProps(f: ((String, Any)) => (String, Any)): R
  def map[K2, V2](f: ((String, Any)) => (K2, V2)): Map[K2, V2]
  def removeAll(keys: IterableOnce[String]): R
  def ++(xs: IterableOnce[(String, Any)]): R
  def remove(key: String): R
  def +(elem: (String, Any)): R
  def +(elems: (String, Any)*): R
  def getOrElse(key: String, default: => Any): Any
  def isDefinedAt(key: String): Boolean
  def mapValuesNow(f: Any => Any): R = {
    val builder = PickledProperties.newBuilder
    iterator.foreach { case (k, v) => builder.addOne(k, f(v)) }
    builder.result().asInstanceOf[R]
  }
  def filterKeysNow(f: String => Boolean): PickledProperties = {
    val builder = PickledProperties.newBuilder
    iterator.foreach { elem =>
      if (f(elem._1))
        builder.addOne(elem)
    }
    builder.result()
  }

  def matches(map: Map[String, Any]): Boolean
  def keySet: Set[String]
  def asMap: Map[String, Any]
  def untagged: R = remove(PicklingConstants.embeddableTag)
  def hasTag: Boolean = contains(PicklingConstants.embeddableTag)
  def tag: String = apply(PicklingConstants.embeddableTag).asInstanceOf[String]
}

trait PickledProperties extends Map[String, Any] with PickledPropertiesApi[PickledProperties] {
  override def mapProps(f: ((String, Any)) => (String, Any)): PickledProperties = PickledProperties(super[Map].map(f))
  override def ++(xs: IterableOnce[(String, Any)]): PickledProperties = PickledProperties(super[Map].++(xs))
  override def +(elem: (String, Any)): PickledProperties = PickledProperties(super[Map].+(elem))
  override def +(elems: (String, Any)*): PickledProperties = PickledProperties(super[Map].++(elems))
  override def getOrElse(key: String, default: => Any): Any = super[Map].getOrElse(key, default)
  override def removeAll(keys: IterableOnce[String]): PickledProperties = PickledProperties(this -- keys)
  override def remove(key: String): PickledProperties = PickledProperties(this - key)
  override def updated[V1 >: Any](key: String, value: V1): Map[String, V1] = {
    val builder = PickledProperties.newBuilder
    builder.addAll(this)
    builder.addOne((key, value))
    builder.result().asMap
  }
  override def removed(key: String): Map[String, Any] = {
    if (key == PicklingConstants.embeddableTag)
      untagged.asMap
    else {
      // really we should only be called to remove the tag but some tests want to remove
      // fields for convenience. In case there are other usages, a reasonable implementation
      // would be to use filterNot. We're not looking to be efficient for such usage.
      PickledProperties(filterNot { case (k, _) => k == key }).asMap
    }
  }
  override def toString: String = {
    mkString("PickledProperties(", ", ", ")")
  }
  override def asMap: Map[String, Any] = this
  def matches(map: Map[String, Any]): Boolean = {
    (this.size == map.size) && {
      try this.forall(kv => map.getOrElse(kv._1, ScalaMapCompatibleHashEquals.DefaultSentinel) == kv._2)
      catch { case _: ClassCastException => false } // PR #9565 / scala/bug#12228
    }
  }
}

object PickledProperties {
  def newBuilder = new PickledPropertiesBuilder
  val empty: PickledProperties = new MapImpl(Map.empty[String, Any])

  /**
   * Use for creating PickledProperties in the map order that would match the order
   * of items when entities are read from the DAL. This is useful for tests that
   * want to assert the contents of pickled forms inclusive of their order.
   */
  def inPicklingOrder(tag: String, elems: (String, Any)*): PickledProperties = {
    val builder = new PickledPropertiesBuilder(PicklingMapEntryOrderWorkaround.newBuilder)
    builder.addOne(PicklingConstants.embeddableTag, tag)
    elems.foreach { e => builder.addOne(e) }
    builder.result()
  }
  def apply(elems: (String, Any)*): PickledProperties = {
    val builder = new PickledPropertiesBuilder()
    elems.foreach { e => builder.addOne(e) }
    builder.result()
  }
  def apply(m: collection.Map[String, Any]): PickledProperties = {
    apply(m.toMap)
  }
  def apply(m: Map[String, Any]): PickledProperties = {
    m match {
      case pp: PickledProperties => pp
      case _                     => new MapImpl(m)
    }
  }

  def checkPickledPropertiesAsMapsAllowed(m: collection.Map[_, _]): Boolean = {
    if (!Settings.allowPickledPropertiesAsMaps) {
      throw new UnsupportedOperationException(s"Map $m provided but PickledProperties expected")
    }
    true
  }

  @SerialVersionUID(1)
  private[pickling] class MapImpl(private val m: Map[String, Any]) extends PickledProperties with Serializable {
    override def hashCode: Int = m.hashCode
    override def knownSize: Int = m.knownSize
    // noinspection ScalaDeprecation
    override def keys: Iterable[String] = m.keys
    override def contains(key: String): Boolean = m.contains(key)
    override def values: Iterable[Any] = m.values
    override def iterator: Iterator[(String, Any)] = m.iterator
    override def get(key: String): Option[Any] = m.get(key)
    override def apply(key: String): Any = m.apply(key)
    override def mapProps(f: ((String, Any)) => (String, Any)): PickledProperties = PickledProperties(m.map(f))
    override def map[K2, V2](f: ((String, Any)) => (K2, V2)): Map[K2, V2] = m.map(f)
    override def removeAll(keys: IterableOnce[String]): PickledProperties = PickledProperties(m -- keys)
    override def ++(xs: IterableOnce[(String, Any)]): PickledProperties = PickledProperties(m ++ xs)
    override def remove(key: String): PickledProperties = PickledProperties(m - key)
    override def +(elem: (String, Any)): PickledProperties = PickledProperties(m + elem)
    override def +(elems: (String, Any)*): PickledProperties = PickledProperties(m ++ elems)
    override def getOrElse(key: String, default: => Any): Any = m.getOrElse(key, default)
    override def isDefinedAt(key: String): Boolean = m.isDefinedAt(key)
    override def keySet: Set[String] = m.keySet
    override def asMap: Map[String, Any] = m
  }
}

class PickledPropertiesBuilder(private val mapBuilder: mutable.Builder[(String, Any), Map[String, Any]])
    extends mutable.Builder[(String, Any), PickledProperties] {

  def this() = {
    // our primary ctor allows providing different builders because we need that to properly
    // support a map operation that does deals with the order-depepdency that is
    // supported by PicklingMapEntryOrderWorkaround. But by default, we'll use the regular
    // Map.newBuilder so existing behavior with respect to map-order is retained.
    this(Map.newBuilder[String, Any])
  }

  override def clear(): Unit = mapBuilder.clear()

  override def result(): PickledProperties = new PickledProperties.MapImpl(mapBuilder.result())
  override def addOne(elem: (String, Any)): PickledPropertiesBuilder.this.type = {
    mapBuilder.addOne(elem)
    this
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////
// Below is an implementation of PickledProperties that extends Iterable[String, Any] without extending
// Map[String, Any]. Our eventual goal is to have PickledProperties not extend Map and so we use the
// below implementation to ensure that remediated modules work with this implementation by
// renaming the traits so that trait PickledPropertiesIterable becomes the implementation in our local
// workspace but and then we can switch it back to the Map implementation when we commit the code back
// This way we can do the remediation in a more granular way.
// See also comments before trait PickledPropertiesApi

// scala Map compatible hashCode/Equals so that our Iterable based implementation has the same hash/equals
// as Map to avoid any issues where order of Map elements might matter.
//
//noinspection ScalaUnusedSymbol
trait ScalaMapCompatibleHashEquals {
  self: Iterable[(String, Any)] =>

  def canEqual(other: Any): Boolean = other match {
    case _: ScalaMapCompatibleHashEquals => true
    case _: Map[_, _]                    => true
    case _                               => false
  }
  override def hashCode(): Int = mapHash(self)

  override def equals(o: Any): Boolean = {
    (this eq o.asInstanceOf[AnyRef]) || (o match {
      case map: PickledProperties with ScalaMapCompatibleHashEquals if map.canEqual(this) =>
        (self.size == map.size) && {
          try this.forall(kv => map.getOrElse(kv._1, ScalaMapCompatibleHashEquals.DefaultSentinel) == kv._2)
          catch { case _: ClassCastException => false } // PR #9565 / scala/bug#12228
        }
      case map: Map[String, Any] @unchecked if PickledProperties.checkPickledPropertiesAsMapsAllowed(map) =>
        (self.size == map.size) && {
          try this.forall(kv => map.getOrElse(kv._1, ScalaMapCompatibleHashEquals.DefaultSentinel) == kv._2)
          catch { case _: ClassCastException => false } // PR #9565 / scala/bug#12228
        }
      case _ =>
        false
    })
  }

  private def mapHash(xs: Iterable[(String, Any)]): Int = {
    if (xs.isEmpty) emptyMapHash
    else {
      class accum extends Function1[(String, Any), Unit] {
        var a, b, n = 0
        var c = 1
        override def apply(e: (String, Any)): Unit = {
          val h = product2Hash(e._1, e._2)
          a += h
          b ^= h
          if (h != 0) c *= h
          n += 1
        }
      }
      val accum = new accum
      var h = mapSeed
      xs.foreach(accum)
      h = MurmurHash3.mix(h, accum.a)
      h = MurmurHash3.mix(h, accum.b)
      h = MurmurHash3.mixLast(h, accum.c)
      MurmurHash3.finalizeHash(h, accum.n)
    }
  }
  private def product2Hash(x: String, y: Any) = {
    var h = MurmurHash3.productSeed
    h = MurmurHash3.mix(h, x.##)
    h = MurmurHash3.mix(h, y.##)
    MurmurHash3.finalizeHash(h, 2)
  }
  private final val mapSeed = "Map".hashCode
  private val emptyMapHash = MurmurHash3.unorderedHash(Nil, mapSeed)
}

private object ScalaMapCompatibleHashEquals {
  def DefaultSentinel = new AnyRef()
}

//noinspection ScalaUnusedSymbol
trait PickledPropertiesIterable
    extends PickledPropertiesApi[PickledProperties]
    with Iterable[(String, Any)]
    with ScalaMapCompatibleHashEquals {

  override def keys: Iterable[String] = iterator.map { _._1 }.toSeq
  override def values: Iterable[Any] = iterator.map { _._2 }.toSeq
  override def apply(key: String): Any = {
    get(key) match {
      case Some(v) => v
      case _       => throw new NoSuchElementException()
    }
  }
  override def mapProps(f: ((String, Any)) => (String, Any)): PickledProperties = {
    val builder = new PickledPropertiesBuilder()
    iterator.foreach { e => builder.addOne(f(e)) }
    builder.result()
  }
  override def map[K2, V2](f: ((String, Any)) => (K2, V2)): Map[K2, V2] = iterator.map(f).toMap
  override def removeAll(keys: IterableOnce[String]): PickledProperties = {
    val ks = keys.iterator.toSet
    val builder = new PickledPropertiesBuilder()
    var removed: Boolean = false
    foreach { elem =>
      if (ks.contains(elem._1))
        removed = true
      else
        builder.addOne(elem)
    }
    if (removed) builder.result() else this.asInstanceOf[PickledProperties]
  }
  override def ++(xs: IterableOnce[(String, Any)]): PickledProperties = {
    val builder = new PickledPropertiesBuilder()
    builder.addAll(this)
    builder.addAll(xs)
    builder.result()
  }
  override def remove(key: String): PickledProperties = {
    var removed: Boolean = false
    val builder = new PickledPropertiesBuilder()
    foreach { elem =>
      if (elem._1 == key)
        removed = true
      else
        builder.addOne(elem)
    }
    if (removed) builder.result() else this.asInstanceOf[PickledProperties]
  }
  override def +(elem: (String, Any)): PickledProperties = {
    val builder = new PickledPropertiesBuilder()
    builder.addAll(this)
    builder.addOne(elem)
    builder.result()
  }
  override def +(elems: (String, Any)*): PickledProperties = {
    val builder = new PickledPropertiesBuilder()
    builder.addAll(this)
    builder.addAll(elems)
    builder.result()
  }
  override def getOrElse(key: String, default: => Any): Any = get(key) match {
    case Some(v) => v
    case None    => default
  }
  override def isDefinedAt(key: String): Boolean = contains(key)
  override def contains(key: String): Boolean = get(key).isDefined
  override def keySet: Set[String] = keys.toSet
  override def asMap: Map[String, Any] = {
    val builder = Map.newBuilder[String, Any]
    builder.addAll(this)
    builder.result()
  }
  override def addString(sb: StringBuilder, start: String, sep: String, end: String): sb.type =
    map { case (k, v) => s"$k -> $v" }.addString(sb, start, sep, end)
  override def toString: String = {
    mkString("PickledProperties(", ",", ")")
  }
  def matches(map: Map[String, Any]): Boolean = {
    (this.size == map.size) && {
      try this.forall(kv => map.getOrElse(kv._1, ScalaMapCompatibleHashEquals.DefaultSentinel) == kv._2)
      catch { case _: ClassCastException => false } // PR #9565 / scala/bug#12228
    }
  }
}
