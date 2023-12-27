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
package optimus.buildtool.utils

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache

import java.net.URL
import java.net.URLClassLoader

import scala.collection.immutable.Seq

/**
 * Typesafe-ish reflection targets.
 *
 * Note that it is not uncommon to compile from a given scala version to another scala version, so these conversions
 * should be limited to standard java objects and interfaces.
 */
object Reflectors {
  import java.util.function.BiConsumer
  import java.util.function.Function

  /**
   * Instantiate a `BiConsumer[A,B]` from an implementation `what`.
   */
  def biConsumer[T, U](what: Class[_ <: BiConsumer[T, U]], classpath: Seq[URL]): BiConsumer[T, U] = {
    new UnsafeReflectingFactory(what, classpath).construct().asInstanceOf[BiConsumer[T, U]]
  }

  /**
   * Instantiate a `Function[A,B]` from an implementation `what`.
   */
  def function[T, R](what: Class[_ <: Function[T, R]], classpath: Seq[URL]): Function[T, R] = {
    new UnsafeReflectingFactory(what, classpath).construct().asInstanceOf[Function[T, R]]
  }
}

/**
 * A factory for creating instances of classes in jars we have compiled.
 *
 * This is useful when we want to use some code in OBT without having to make it an OBT dependency and thus dependent on
 * the OBT release cycle.
 */
private class UnsafeReflectingFactory(what: Class[_], classpath: Seq[URL]) {
  import UnsafeReflectingFactory._

  /**
   * Create a new instance of `what` using the constructor parameters in `args`.
   *
   * The constructor parameters are tuples of Class[A] -> A. Note that they aren't typechecked against `what`!
   *
   * This isn't very safe! You should if possible use the safer stuff in [[Reflectors]].
   */
  def construct(args: ({ type pair[A] = (Class[_ <: A], A) })#pair[_]*): Any = {
    val (argsT, argsV) = args.map { case (t, v) =>
      t.asInstanceOf[Class[AnyRef]] -> v.asInstanceOf[AnyRef]
    }.unzip
    val loader = createIsolatedClassloader(classpath, what)
    val targetClass = loader.loadClass(what.getName)
    targetClass.getDeclaredConstructor(argsT: _*).newInstance(argsV: _*)
  }
}

private object UnsafeReflectingFactory {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)

  // not using an optimus cache here, because during tests that gets cleared, and for whatever
  // reason I cannot cajole the JVM into reliably cleaning up the associated metaspace usage.
  // This cache really should only have one element anyways, possibly 2 if you are switching
  // between two scala versions.
  private val classloaderCache: LoadingCache[(Seq[URL], Class[_]), URLClassLoader] = Caffeine
    .newBuilder()
    .build(key => createIsolatedClassloaderSync(key._1, key._2))

  private def createIsolatedClassloader(classpath: Seq[URL], compiler: Class[_]): ClassLoader =
    classloaderCache.get((classpath, compiler))

  private def createIsolatedClassloaderSync(classpathUrls: Seq[URL], compiler: Class[_]): URLClassLoader = {
    // inconveniently the overload of loadClass which takes the resolve parameter is protected on ClassLoader,
    // so we have to do a little dance here to expose it, so that we can call it below.
    class ExposedClassLoader(parent: ClassLoader) extends ClassLoader(parent) {
      override def loadClass(name: String, resolve: Boolean): Class[_] = super.loadClass(name, resolve)
    }
    val outerExposedClassloader = new ExposedClassLoader(getClass.getClassLoader)

    // note that we have null parent, so classes will really get loaded in this classloader
    new URLClassLoader(classpathUrls.toArray, null) {
      override def loadClass(name: String, resolve: Boolean): Class[_] = {
        // we load the base java classes in the shared outer classloader (so that we can interoperate using java types)
        // but load everything else in this classloader, which only has Scalac and this code on the classpath. That
        // isolates Scalac from this version of Optimus, and forces Scalac to dynamically load the version
        // we have compiled instead.
        if (name.startsWith("java")) outerExposedClassloader.loadClass(name, resolve)
        else super.loadClass(name, resolve)
      }
    }
  }
}
