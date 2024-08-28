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

import optimus.core.utils.RuntimeMirror

import scala.reflect.runtime.universe._

/**
 * Unfortunately, the Scala Reflection API is not thread safe, so we need to ensure we lock before accessing it. This
 * object contains all the scala reflection logic for the generation of (un)picklers at runtime
 */
private[pickling] object PicklingReflectionUtils {

  private val cls = getClass
  private val mirror = RuntimeMirror.forClass(cls)

  def classToType(clazz: Class[_]): Type = mirror.classSymbol(clazz).toType

  def extractName(tpe: Type): String = {
    // Scala enums require to consider the name of the object, rather than the class itself.
    // This avoid all the enums to have a correct extracted name rather than "class Value"
    val extractedType = if (isScalaEnum(tpe)) getOwnerCompanion(tpe) else tpe
    extractName(typeSymbol(extractedType))
  }

  def extractName(symbol: Symbol): String = {
    info(symbol) match {
      case rt: RefinedType => parents(rt).map(extractName).mkString(" with ")
      case _               =>
        // Using fullName as key to avoid bug related to de-aliasing such as https://github.com/scala/bug/issues/6474
        fullName(symbol)
    }
  }

  def baseClasses(tpe: Type): List[Symbol] = tpe.baseClasses

  def companionObjectInstance(tpe: Type): Any = {
    val tpeName = tpe.typeSymbol.fullName
    // [SEE_INNER_EMBEDDABLE_TRAIT] avoid recursive update during initialisation triggered by RuntimeMirror.forClass
    // (in fallback case)
    RuntimeMirror.moduleByName(tpeName, cls, cached = false)
  }

  private def getOwnerCompanion(tpe: Type): Type = tpe match {
    case RefinedType(parent :: _, _) => getOwnerCompanion(parent)
    case _                           => getUnderlyingType(tpe.asInstanceOf[TypeRef].pre)
  }

  def getUnderlyingType(tpe: Type): Type = {
    import scala.reflect.internal.{Types => ITypes}
    val internalType = tpe.asInstanceOf[ITypes#Type].underlying
    internalType.asInstanceOf[Type]
  }

  def getTypeArgs(tpe: Type): List[Type] = tpe.dealias.typeArgs

  def extractCtorArgs(tpe: Type): List[(String, Type)] = {
    tpe.typeSymbol.asClass.primaryConstructor.asMethod.infoIn(tpe).paramLists.head.map { symbol =>
      val symbolName = symbol.name.decodedName.toString
      val symbolType = symbol.info.typeSymbol.info match {
        case bounds: TypeBounds => bounds.hi
        case _                  => symbol.typeSignature
      }
      symbolName -> symbolType
    }
  }

  def manifestOf[T](tp: Type): Manifest[T] = {

    def createManifestFromClassType(tpe: Type, targs: List[Manifest[_]] = Nil) = targs match {
      case Nil          => Manifest.classType(mirror.runtimeClass(tpe))
      case head :: tail => Manifest.classType(mirror.runtimeClass(tpe), head, tail: _*)
    }

    def createManifestFromTypeSymbolInfo(tpe: Type, targs: List[Manifest[_]] = Nil) = tpe.typeSymbol.info match {
      case TypeBounds(lo, hi) =>
        val lowerBound = loop(lo)
        val upperBound = loop(hi)
        Manifest.wildcardType(lowerBound = lowerBound, upperBound = upperBound)
      case _ => createManifestFromClassType(tpe, targs)
    }

    def loop(tpe: Type): Manifest[_] = tpe match {
      case RefinedType(parents, decls) if decls.isEmpty =>
        val parentManifests = parents.map(loop)
        Manifest.intersectionType(parentManifests: _*)
      case ExistentialType(_, underlying) => createManifestFromTypeSymbolInfo(underlying)
      case t if t =:= typeOf[Nothing]     => Manifest.Nothing
      case t if t =:= typeOf[Any]         => Manifest.Any
      case t if isScalaEnum(t)            => manifestOf(getOwnerCompanion(t))
      case TypeRef(_, _, targs)           => createManifestFromTypeSymbolInfo(tpe, targs.map(loop))
      case _                              => createManifestFromClassType(tpe)
    }

    loop(tp).asInstanceOf[Manifest[T]]
  }

  private val enumerationTpe = typeOf[Enumeration#Value]
  def isScalaEnum(tpe: Type): Boolean = tpe <:< enumerationTpe

  def getEncodedName(tpe: Type): String =
    tpe.typeSymbol.asClass.name.encodedName.toString

  def tpeDeepDealias(tpe: Type): Type = tpe.map(t => t.dealias)

  private def info(symbol: Symbol): Type = symbol.info

  private def typeSymbol(tpe: Type): Symbol = tpe.typeSymbol

  private def parents(rt: RefinedType): List[Type] = rt.parents

  private def fullName(symbol: Symbol): String = symbol.fullName

  private def typeSignature(module: ModuleSymbol): Type = module.typeSignature

  def safeAppliedType[T](weakTypeTag: WeakTypeTag[T], args: List[Type]): Type = appliedType(weakTypeTag.tpe, args)
}
