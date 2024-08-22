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
package optimus.platform.versioning

import optimus.platform.annotations.tweakable
import optimus.platform._

private[optimus] trait TypeInfoApiBase {
  self: VersioningUtilsBase =>

  implicit def toTypeRef(t: u.TypeRefApi): u.TypeRef = t.asInstanceOf[u.TypeRef]
  implicit def toTypeRef(t: u.RefinedTypeApi): u.RefinedType = t.asInstanceOf[u.RefinedType]

  sealed trait FieldModifier
  object FieldModifier {
    case object Key extends FieldModifier
    case object BackedDef extends FieldModifier
  }

  case class FieldMetadata(name: String, rft: RegisteredFieldType, mods: Set[FieldModifier]) {
    val ft = rft.toFieldType()
    def symbolMatches(other: Map[String, FieldMetadata]): Boolean = {
      other.contains(name) && other(name).ft == ft
    }
    def isKey: Boolean = mods contains FieldModifier.Key
  }

  protected abstract class TypeInfoBase protected (protected val concreteType: u.Type) {
    protected def storableTypeName: u.Name
    protected def companionType: u.Type
    protected def isKey(s: u.Symbol): Boolean
    protected def isBackedDef(s: u.Symbol): Boolean
    protected def getStoredVal(member: u.Symbol): Option[FieldMetadata]
    val isEmbeddable: Boolean = false
    protected val members: Seq[u.Symbol]
    val companionTypeAllStoredProperties: Map[String, FieldMetadata]
    protected def classType: u.Type

    def isCanonical: Boolean = {
      // NB we can't actually check the full FieldMetadata because there are bits of (non-crucial) information which can
      // get lost before versioning macros kick in. Namely things like this:
      //   @stored @entity class Foo(@key val name: String); object Foo { type S = { val name: String @key } }
      // By the time we come to inspect the type of `val name` on the refined type S the annotation of String with @key
      // has been lost in typing S. That means that it isn't detected as a key. This is typically non-problematic since
      // the key info is only confidence checking anyway. But a problem does arise in the case of the canonical check if we
      // include that FieldModifier in the check because it means that !shapeOf[Foo.S].isCanonical, which is incorrect.
      // For this reason we exclude everything but name and fieldType for the isCanonical check.
      val sp = allStoredProperties.map { case (name, meta) => name -> meta.ft }
      val csp = companionTypeAllStoredProperties.map { case (name, meta) => name -> meta.ft }
      // TODO (OPTIMUS-14927): Also include type comparison to make canonical decision for embeddable type
      if (isEmbeddable)
        sp.keys.toSet == csp.keys.toSet
      else
        sp == csp
    }

    final lazy val baseClassLookup = classType.baseClasses.zipWithIndex.toMap

    final lazy val storedVals: Map[String, FieldMetadata] = {
      val metadata = members.flatMap(m => getStoredVal(m).map(f => (m, f)))
      getNameToFieldMetadata(metadata)
    }

    final lazy val backedDefs: Map[String, FieldMetadata] = {
      val metadata = members.collect {
        case m if isBackedDef(m) =>
          val md = fieldMetadata(m)
          m -> md
      }
      getNameToFieldMetadata(metadata)
    }

    private def getNameToFieldMetadata(metadata: Seq[(u.Symbol, FieldMetadata)]) = {
      metadata
        .groupBy { case (_, field) => field.name }
        .map { case (name, values) =>
          val (_, fieldMetadata) = values.minBy {
            // It will give the most generic field type since baseClasses is ordered
            case (symbol, _) => baseClassLookup(symbol.owner)
          }
          name -> fieldMetadata
        }
    }

    final lazy val keys: Map[String, FieldMetadata] = storedVals.filter { case (name, meta) =>
      meta.mods.contains(FieldModifier.Key)
    }

    final lazy val allStoredProperties: Map[String, FieldMetadata] = storedVals ++ backedDefs

    final def keysEqual(other: TypeInfoBase): Boolean = {
      val k1 = keys
      val k2 = other.keys
      k1.values.forall(_.symbolMatches(k2)) && k2.values.forall(_.symbolMatches(k1))
    }

    final def sameShapeAs(other: TypeInfoBase): Boolean = {
      storedVals.values.forall(_.symbolMatches(other.storedVals)) && other.storedVals.values
        .forall(_.symbolMatches(storedVals))
    }

    def getFieldType(field: String): Option[u.Type] =
      members.find(m => m.name.stringify == field).map(_.typeSignature.dealias)

    final def containsField(field: String): Boolean = {
      val backedField = backed(field)
      storedVals.contains(field) || backedDefs.contains(backedField)
    }

    // Set of "normalized" field names..
    final def fieldSet: Set[String] = allStoredProperties.keySet.map(unbacked)

    final def isBackedDef(field: String): Boolean = {
      val backedField = backed(field)
      backedDefs.contains(backedField)
    }

    private final def fieldMetadata(s: u.Symbol): FieldMetadata = {
      val mods = fieldModifiers(s)
      val tpe = s.typeSignature
      val (nme, fieldType) = if (mods contains FieldModifier.BackedDef) {
        val nme = backed(s.name.stringify)
        val fieldType = RegisteredFieldType.Option(tpe.fieldType)
        (nme, fieldType)
      } else {
        val nme = s.name.stringify
        val fieldType = tpe.fieldType
        (nme, fieldType)
      }
      FieldMetadata(nme, fieldType, mods)
    }

    protected final def fieldModifiers(s: u.Symbol): Set[FieldModifier] = {
      val ret = Set.newBuilder[FieldModifier]
      if (isKey(s)) ret += FieldModifier.Key
      if (isBackedDef(s)) ret += FieldModifier.BackedDef
      ret.result()
    }
  }

  abstract class CompanionTypeInfoBase(moduleType: u.Type) extends TypeInfoBase(moduleType) {
    protected override def getStoredVal(member: u.Symbol): Option[FieldMetadata] = {
      if (member.hasAnnotation[stored]) {
        member match {
          case s if !s.hasAnnotation[backed] && !s.isSynthetic =>
            // This is the "normal" case of a @stored val. We use !isSynthetic to rule out var blah$impl on tweakable nodes.
            // TODO (OPTIMUS-13077): we should fix this not to rely on the synthetic flag
            val name = s.name.stringify
            val mods = fieldModifiers(s)
            val fieldType = s.typeSignature.fieldType
            Some(FieldMetadata(name, fieldType, mods))

          case s
              if s.isSynthetic &&
                (s.name.stringify.endsWith("$impl") || s.name.stringify.endsWith("$backing")) =>
            // The above doesn't pick up on e.g. tweakable nodes inherited from already compiled entities, in which case we see:
            //  @stored var foo$impl
            //  @node @tweakable def foo
            val fieldType = s.typeSignature match {
              case tr: u.TypeRefApi if tr.args.size == 1 =>
                if (tr.sym == types.node.typeSymbol) tr.args.head.fieldType
                else tr.asInstanceOf[u.TypeRef].fieldType
              case o => o.fieldType
            }
            val nameStr = s.name.stringify
            val name = nameStr.substring(0, nameStr.lastIndexOf('$'))
            val mods = fieldModifiers(members.find(_.name.stringify == name).getOrElse(s))
            Some(FieldMetadata(name, fieldType, mods))

          case s if s.hasAnnotation[tweakable] && !s.hasAnnotation[backed] =>
            val name = s.name.stringify
            val mods = fieldModifiers(s)
            Some(FieldMetadata(name, s.typeSignature.fieldType, mods))

          case _ => None
        }
      } else
        None
    }

    // The set of properties which are marked @backed (not including compiler generated backed$foo values)
    protected override def isBackedDef(m: u.Symbol): Boolean =
      m.hasAnnotation[backed] && m.isMethod && !m.name.stringify.startsWith("backed$")
    protected override def isKey(m: u.Symbol): Boolean =
      m.hasAnnotation[key] || (m.isTerm && m.asTerm.getter != u.NoSymbol && m.asTerm.getter.hasAnnotation[key])

    lazy override val companionTypeAllStoredProperties: Map[String, FieldMetadata] = allStoredProperties

    protected override def companionType: u.Type = moduleType
  }

  trait CompanionTypeInfoApi {
    protected val classType: u.Type
    protected def storableParentTypeSymbols: Seq[u.Type]

    protected final def getMembers: Seq[u.Symbol] = {
      val storableParents = storableParentTypeSymbols
      val allDecs = storableParents flatMap { _.decls }
      // This hack is here to force annotation info to get loaded; otherwise the compiler might just decide not to give it to us
      allDecs foreach { _.typeSignature }
      allDecs
    }

    protected final def getStorableTypeName: u.Name = classType.typeSymbol.typeName
  }

  trait EntityTypeInfoApi extends CompanionTypeInfoApi {
    self: TypeInfoBase =>
    final override protected def storableParentTypeSymbols: Seq[u.Type] = {
      // NB there is a reason not to use .members here. For some reason in this case:
      //   @entity trait T { val x: Int = 50 }
      //   @entity class C() extends T
      //   typeOf[C].members does not include val x
      // TODO (OPTIMUS-19349): If you can figure out why, feel free to fix this code.
      // As of 2.11, it appears that members *does* include x, so this code could possibly be simplified a great deal.
      classType.baseClasses.filter { _.typeSignature.typeSymbol.hasAnnotation[entity] }.map { _.typeSignature }
    }

    protected final override val members = getMembers
    final override def storableTypeName = getStorableTypeName
  }

  trait BusinessEventTypeInfoApi extends CompanionTypeInfoApi {
    self: TypeInfoBase =>
    final override protected def storableParentTypeSymbols: Seq[u.Type] = {
      // NB there is a reason not to use .members here. For some reason in this case:
      //   @event trait T { val x: Int = 50 }
      //   @event class C() extends T
      //   typeOf[C].members does not include val x
      // If you can figure out why, feel free to go nuts on this code.
      classType.baseClasses.filter { _.typeSignature.typeSymbol.hasAnnotation[event] }.map { _.typeSignature }
    }

    protected final override val members = getMembers
    final override def storableTypeName = getStorableTypeName
  }

  trait EmbeddableTypeInfoApi extends CompanionTypeInfoApi {
    self: TypeInfoBase =>
    final override protected def storableParentTypeSymbols: Seq[u.Type] = Seq(classType)

    override val isEmbeddable: Boolean = true
    protected final override val members = getMembers
    final override def storableTypeName = getStorableTypeName
    // Embeddable doesn't have stored annotation so we need to take all val which are part of constructor
    final override protected def getStoredVal(member: u.Symbol): Option[FieldMetadata] = member match {
      case s: u.MethodSymbolApi if s.isParamAccessor =>
        val name = s.name.stringify
        val mods = fieldModifiers(s.asInstanceOf[u.Symbol])
        Some(FieldMetadata(name, s.returnType.asSeenFrom(classType, classType.typeSymbol).fieldType, mods))
      case _ => None
    }
  }
}
