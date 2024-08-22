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
package optimus.platform.util.dalschema

import optimus.platform.indexed
import optimus.platform.projected

import scala.annotation.tailrec
import scala.reflect.runtime.universe._

trait Association {
  def e: ClassInfo
  def allReferences: Seq[ClassInfo] = Seq(e)
}

object Association {
  final case class Inline(e: ClassInfo) extends Association
  final case class OneToOne(e: ClassInfo) extends Association
  final case class OneToZeroOrOne(e: ClassInfo) extends Association
  final case class OneToMany(e: ClassInfo) extends Association
  final case class ManyToMany(e: ClassInfo, otherType: ClassInfo, isInverse: Boolean) extends Association {
    override def allReferences: Seq[ClassInfo] = Seq(e, otherType)
  }
}

trait FieldInfo {
  def name: String
  def returnType: ClassInfo
  def owner: ClassInfo
  def isPublic: Boolean
  def isKey: Boolean
  def isIndexed: Boolean
  def isIndexedUnique: Boolean
  def isIndexedQueryable: Boolean
  def isIndexedQueryByEref: Boolean
  def association(considerTrivialIf: ClassInfo => Boolean = _ => false): Association
  def isProjected: Boolean
  def isProjectedIndexed: Boolean
  def isProjectedQueryable: Boolean
  def annotations: List[AnnotationInfo]
}

private[dalschema] final case class EnumerationFieldInfo(name: String, owner: ClassInfo) extends FieldInfo {
  import Association._
  override def isPublic: Boolean = true
  override def isKey: Boolean = false
  override def isIndexed: Boolean = false
  override def isIndexedUnique: Boolean = false
  def isIndexedQueryable: Boolean = false
  def isIndexedQueryByEref: Boolean = false
  override def association(considerTrivialIf: ClassInfo => Boolean = _ => false): Association = Inline(returnType)
  override def returnType: ClassInfo = owner
  def isProjected: Boolean = false
  def isProjectedIndexed: Boolean = false
  def isProjectedQueryable: Boolean = false
  def annotations: List[AnnotationInfo] = List.empty[AnnotationInfo]
}

private[dalschema] final case class SymbolFieldInfo(term: TermSymbol, containingType: Type, includeTypeArgs: Boolean)
    extends FieldInfo {
  import Association._

  def name: String = term.name.decodedName.toString.stripSuffix("$impl ")
  def returnType: ClassInfo = {
    ClassInfo(term.typeSignatureIn(containingType).finalResultType, includeTypeArgs)
  }
  def owner: ClassInfo = {
    @tailrec
    def findOwningClass(sym: Symbol): ClassSymbol = {
      if (sym.owner.isClass) {
        sym.owner.asClass
      } else {
        findOwningClass(sym.owner)
      }
    }
    ClassInfo(findOwningClass(term).toType)
  }
  def isPublic: Boolean = term.isPublic
  def isKey: Boolean = ReflectionHelper.isKey(term)

  def isIndexed: Boolean = ReflectionHelper.isIndexed(term)

  private def getAnnotationParameterValue(annot: Type, param: String): Option[Any] = {
    ReflectionHelper.getAnnotationParameterValue(term, annot, param)
  }

  def isIndexedUnique: Boolean = {
    getAnnotationParameterValue(typeOf[indexed], "unique").exists(_.asInstanceOf[Boolean])
  }

  def isIndexedQueryable: Boolean = {
    // forAll here because the default for Queryable is true
    getAnnotationParameterValue(typeOf[indexed], "queryable").forall(_.asInstanceOf[Boolean])
  }

  def isIndexedQueryByEref: Boolean = {
    getAnnotationParameterValue(typeOf[indexed], "queryByEref").exists(_.asInstanceOf[Boolean])
  }

  def isProjected: Boolean = ReflectionHelper.isProjected(term)

  def isProjectedIndexed: Boolean = {
    getAnnotationParameterValue(typeOf[projected], "indexed").exists(_.asInstanceOf[Boolean])
  }
  def isProjectedQueryable: Boolean = {
    // forall here because the default for Queryable is true
    getAnnotationParameterValue(typeOf[projected], "queryable").forall(_.asInstanceOf[Boolean])
  }

  def annotations: List[AnnotationInfo] = {
    term.annotations.map { AnnotationInfo(_) } ++
      // look at base class fields we might be overriding...
      term.overrides.flatMap { sym =>
        sym.annotations
          // Filter only for inheritable annotations.
          .filter { a =>
            ReflectionHelper.isInheritedAnnotation(a.tree.tpe)
          }
          .map { AnnotationInfo(_, Option(SymbolFieldInfo(sym.asTerm, sym.owner.info, includeTypeArgs))) }
      }
  }

  def association(considerTrivialIf: ClassInfo => Boolean = _ => false): Association = {
    def isNontrivialType(e: ClassInfo) = {
      (e.isEntity || e.isEvent || e.isEmbeddable || e.isEnumeration) && !considerTrivialIf(e)
    }
    returnType match {
      case e: ClassInfo if e.isMap =>
        val keyType = e.typeArgs.head.unwrapped
        val valType = e.typeArgs.last.unwrapped
        if (isNontrivialType(keyType)) {
          ManyToMany(keyType, valType, isInverse = false)
        } else if (isNontrivialType(valType)) {
          ManyToMany(valType, keyType, isInverse = true)
        } else {
          Inline(e)
        }
      case e: ClassInfo if e.isOption || e.isKnowable =>
        val ue = e.unwrapped
        if (isNontrivialType(ue)) OneToZeroOrOne(ue) else Inline(e)
      case e: ClassInfo if e.isCollection =>
        val ue = e.unwrapped
        if (isNontrivialType(ue)) OneToMany(ue) else Inline(e)
      case e: ClassInfo if isNontrivialType(e) =>
        OneToOne(e)
      case e: ClassInfo => Inline(e)
    }
  }
}
