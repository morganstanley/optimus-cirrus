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
package optimus.profiler.replUtil

import optimus.core.utils.RuntimeMirror

import java.util.{HashMap => JHashMap}
import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object ClassUtils {

  type JDouble = java.lang.Double
  type JFloat = java.lang.Float
  type JLong = java.lang.Long
  type JInt = java.lang.Integer
  type JShort = java.lang.Short
  type JByte = java.lang.Byte
  type JChar = java.lang.Character
  type JBoolean = java.lang.Boolean

  implicit class ClassExtensionsForAny[A](val o: A) extends AnyVal {
    def asAny: Any = o.asInstanceOf[Any]
    def asAnyRef: AnyRef = o.asInstanceOf[AnyRef]
    def clazz: Class[Any] = (if (o != null) o.getClass else classOf[Null]).ofAny // should work on nulls too
  }

  implicit class ClassExtensionsForClass[A](val c: Class[A]) extends AnyVal {
    def ofAny: Class[Any] = c.asInstanceOf[Class[Any]]
    def ofAnyRef: Class[AnyRef] = c.asInstanceOf[Class[AnyRef]]
  }

  implicit class ClassExtensionsForString(val s: String) extends AnyVal {
    def c: Class[_] = Class.forName(s)
  }

  def clazzOf[A: ClassTag]: Class[A] =
    implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]

}

final case class ClassMap[A](entries: (Class[_], A)*) {
  import ClassUtils._
  private val cache = new JHashMap[Class[_], AnyRef](entries.length) // it's faster than the scala equivalent
  entries foreach { case (clazz, value) => cache.put(clazz, value.asAnyRef) }

  def +(entries: (Class[_], A)*): ClassMap[A] =
    this.++(entries)

  def ++(entries: Iterable[(Class[_], A)]): ClassMap[A] =
    ClassMap(this.entries ++ entries: _*)

  def -(classPred: Class[_] => Boolean): ClassMap[A] =
    ClassMap(this.entries.filterNot { case (c, _) => classPred(c) }: _*)

  def -(classes: Class[_]*): ClassMap[A] =
    this.--(classes)

  def --(classes: Iterable[Class[_]]): ClassMap[A] = {
    val set = classes.toSet
    this.-(set.contains _)
  }

  def get(key: Class[_]): Option[A] = {
    val value = getOrFind(key)
    if (value != NoValue) Some(value.asInstanceOf[A]) else None
  }

  def getOrElse(key: Class[_], default: => A): A = {
    val value = getOrFind(key)
    if (value != NoValue) value.asInstanceOf[A] else default
  }

  private val rtMirror = RuntimeMirror.forClass(getClass)

  private def getOrFind(key: Class[_]): AnyRef = {
    val value = cache.computeIfAbsent(
      key,
      _ => {
        if (key != null) { // nulls cannot be stored (NoValue is returned)
          try { // executed outside of Optimus runtime context -> safe to use Scala try
            val keySymbol = rtMirror.classSymbol(key)
            val keyClassSymbol = keySymbol.asClass
            val baseClasses = keyClassSymbol.baseClasses // <- this can throw because of a bug
            val filteredBaseClasses = baseClasses.filterNot { symbol =>
              (symbol.asClass.toType =:= typeOf[Any]) || (symbol eq keySymbol)
            }
            val bases = filteredBaseClasses.map(_.asClass)
            bases.toStream
              .map(rtMirror.runtimeClass)
              .map(cache.getOrDefault(_, NotFound))
              .collectFirst { case v if v != NotFound => v }
              .getOrElse(NoValue)
          } catch {
            case _: IndexOutOfBoundsException => // see - https://github.com/scala/bug/issues/10880
              findFallbackJava(key)
          }
        } else NoValue
      }
    )
    cache.put(key, value)
    value
  }

  private def findFallbackJava(c: Class[_]): AnyRef = {
    val superValue = cache.getOrDefault(c.getSuperclass, NoValue)
    if (superValue == NoValue)
      c.getInterfaces.toStream
        .map(findFallbackJava)
        .collectFirst { case w if w != NoValue => w }
        .getOrElse(NoValue)
    else {
      superValue
    }
  }

  private object NotFound
  private object NoValue
}

final case class ClassSet(classes: Class[_]*) extends (Class[_] => Boolean) {
  private val map = new ClassMap[Boolean](classes.map(_ -> true): _*)

  override def apply(c: Class[_]): Boolean =
    map.getOrElse(c, false)

  def +(classes: Class[_]*): ClassSet =
    this.++(classes)

  def ++(classes: Iterable[Class[_]]): ClassSet =
    ClassSet(this.classes ++ classes: _*)

  def -(classPred: Class[_] => Boolean): ClassSet =
    ClassSet(this.classes.filterNot(c => classPred(c)): _*)

  def -(classes: Class[_]*): ClassSet =
    this.--(classes)

  def --(classes: Iterable[Class[_]]): ClassSet = {
    val set = classes.toSet
    this.-(set.contains _)
  }

}

final case class ClassPredMap[A](entries: (Class[_] => Boolean, A)*) {
  import ClassUtils._

  private val superClasses =
    ClassMap(entries.collect { case (<:<(clazz), e) => clazz -> e.asAnyRef }: _*)

  private val predicates =
    entries.collect {
      case (p, e) if !p.isInstanceOf[=:=] && !p.isInstanceOf[<:<] => p -> e.asAnyRef
    }.toArray

  private val cache = new JHashMap[Class[_], AnyRef](entries.length)
  entries.collect { case (=:=(clazz), e) =>
    clazz -> e
  } foreach { case (c, e) =>
    cache.put(c, e.asAnyRef)
  }

  def +(entries: (Class[_] => Boolean, A)*): ClassPredMap[A] =
    this.++(entries)

  def ++(entries: Iterable[(Class[_] => Boolean, A)]): ClassPredMap[A] =
    ClassPredMap(this.entries ++ entries: _*)

  def -(classes: Class[_]*): ClassPredMap[A] =
    this.--(classes)

  def --(classes: Iterable[Class[_]]): ClassPredMap[A] = {
    val set = classes.toSet
    ClassPredMap((this.entries filterNot {
      case (<:<(clazz), _) => set.contains(clazz)
      case (=:=(clazz), _) => set.contains(clazz)
      case _               => false
    }): _*)
  }

  def -(entryIndex: Int): ClassPredMap[A] =
    ClassPredMap(
      (if (entryIndex < entries.length)
         entries.take(entryIndex) ++ entries.drop(entryIndex + 1)
       else
         entries.toSeq): _*)

  def get(key: Class[_]): Option[A] = {
    val value = getOrFind(key)
    if (value != NoValue) Some(value.asInstanceOf[A]) else None
  }

  def getOrElse(key: Class[_], default: => A): A = {
    val value = getOrFind(key)
    if (value != NoValue) value.asInstanceOf[A] else default
  }

  private def getOrFind(key: Class[_]): AnyRef = {
    val value = cache.getOrDefault(key, NotFound)
    if (value != NotFound) value
    else {
      val forSuperClasses = superClasses.getOrElse(key, NotFound)
      if (forSuperClasses != NotFound) {
        cache.put(key, forSuperClasses)
        forSuperClasses
      } else {
        var i = 0 // fast iteration on array
        while (i < predicates.length) {
          val (predicate, value) = predicates(i)
          if (predicate(key)) {
            cache.put(key, value)
            return value // break-out with the result
          }
          i += 1
        }
        cache.put(key, NoValue)
        NoValue
      }
    }
  }

  private object NotFound
  private object NoValue
}

final case class ClassPredSet(predicates: (Class[_] => Boolean)*) extends (Class[_] => Boolean) {
  private val map = new ClassPredMap[Boolean](predicates.map(_ -> true): _*)

  override def apply(c: Class[_]): Boolean =
    map.getOrElse(c, false)

  def +(predicates: (Class[_] => Boolean)*): ClassPredSet =
    this.++(predicates)

  def ++(predicates: Iterable[Class[_] => Boolean]): ClassPredSet =
    ClassPredSet(this.predicates ++ predicates: _*)

}

final case class =:=(clazz: Class[_]) extends (Class[_] => Boolean) {
  override def apply(c: Class[_]): Boolean = clazz eq c
  override def toString: String = s"=:=(${clazz.getName})"
}

object =:= {
  def apply[A: ClassTag]: =:= = new =:=(implicitly[ClassTag[A]].runtimeClass)
  def apply(n: String): =:= = new =:=(Class.forName(n))
}

final case class <:<(clazz: Class[_]) extends (Class[_] => Boolean) {
  override def apply(c: Class[_]): Boolean = clazz isAssignableFrom c
  override def toString: String = s"<:<(${clazz.getName})"
}

object <:< {
  def apply[A: ClassTag]: <:< = new <:<(implicitly[ClassTag[A]].runtimeClass)
  def apply(n: String): <:< = new <:<(Class.forName(n))
}
