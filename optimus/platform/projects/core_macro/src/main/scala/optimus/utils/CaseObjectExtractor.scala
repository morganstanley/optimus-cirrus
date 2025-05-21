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
package optimus.utils

import scala.reflect.macros.blackbox
import scala.reflect.macros.blackbox.Context

object CaseObjectExtractor {
  def extractFieldValueToCaseObjects[K, T](extractor: T => K): Map[K, T] =
    macro CaseObjectExtractorImpl.getClassNamesImpl[K, T]
}

object CaseObjectExtractorImpl {

  def getClassNamesImpl[K: c.WeakTypeTag, T: c.WeakTypeTag](c: blackbox.Context)(
      extractor: c.Expr[T => K]): c.Expr[Map[K, T]] = {
    import c.universe._

    val tpe = weakTypeOf[T]
    val sym = tpe.typeSymbol

    if (!sym.isModule && !sym.isClass) c.abort(c.enclosingPosition, s"${sym.fullName} must be an object")

    val fieldName = extractor.tree match {
      case Function(_, Select(_, name: TermName)) => name
      case _ => c.abort(c.enclosingPosition, s"Extractor must access symbol ${extractor.toString}")
    }

    val members = tpe.companion.members.collect {
      case clazz if clazz.isModule =>
        val tpe = clazz.typeSignature.widen
        val clazzInstance = q"$clazz.asInstanceOf[$tpe]"
        val key = q"$clazzInstance.$fieldName"
        q"($key, $clazzInstance)"
    }

    c.Expr[Map[K, T]](q"Map(..$members)")
  }
}
