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

import optimus.core.utils.RuntimeMirror
import optimus.datatype.DatatypeUtil
import optimus.platform._
import optimus.platform.annotations.valAccessor
import optimus.platform.cm.Knowable
import optimus.platform.metadatas.internal.ClassMetaData
import optimus.platform.metadatas.internal.MetaAnnotationAttributes
import optimus.platform.metadatas.internal.PIIDetails
import optimus.platform.util.ClassLoaderHook
import optimus.platform.util.CurrentClasspathResourceFinder
import optimus.platform.util.EmbeddableHierarchyManager
import optimus.platform.util.EventHierarchyManager
import optimus.platform.util.Log
import optimus.platform.util.MetaHierarchyManager
import optimus.platform.util.ResourceFinder
import optimus.platform.util.StoredHierarchyManager
import optimus.platform.util.EntityHierarchyManager
import optimus.scalacompat.collection._
// Using mutable structures when building the
// relationship graphs
import scala.collection.mutable
import scala.reflect.runtime._
import scala.reflect.runtime.universe._
import scala.util.Try

trait ClassInfo {
  def isTrait: Boolean
  def isAbstract: Boolean
  def isObject: Boolean
  def isEvent: Boolean
  def isEmbeddable: Boolean
  def isEntity: Boolean
  def isMeta: Boolean
  def isEnumeration: Boolean
  def isJavaEnumeration: Boolean
  def isFinal: Boolean = {
    classSymbol.isFinal
  }
  def isProjected: Boolean = annotations.exists { a =>
    (a.isType(typeOf[stored]) || a.isType(typeOf[embeddable])) &&
    a.parameters.exists { case (name, value) => name == "projected" && value.asInstanceOf[Boolean] }
  }
  def isContained: Boolean = annotations.exists { a =>
    a.isType(typeOf[event]) &&
    a.parameters.exists { case (name, value) => name == "contained" && value.asInstanceOf[Boolean] }
  }
  def isOption: Boolean
  def isKnowable: Boolean
  def isCollection: Boolean
  def isMap: Boolean
  def isKnownNonStorable: Boolean
  def localName(showTypeArgs: Boolean = false): String
  def directParents(implicit f: ClassInfo => Boolean = _ => true): Iterable[ClassInfo]
  def parents(implicit f: ClassInfo => Boolean = _ => true): Iterable[ClassInfo]
  def selfAndParents(implicit f: ClassInfo => Boolean = _ => true): Iterable[ClassInfo]
  def children(implicit f: ClassInfo => Boolean = _ => true): Iterable[ClassInfo]
  def directChildren(implicit f: ClassInfo => Boolean = _ => true): Iterable[ClassInfo]
  def fields(includeTypeArgs: Boolean = true): Iterable[FieldInfo]
  def fullName: String
  def className: String
  def typeArgs: List[ClassInfo]
  def namespace: String = {
    val name = className
    val dotPos = name.lastIndexOf(".")
    if (dotPos == -1) {
      "default"
    } else {
      name.substring(0, dotPos)
    }
  }
  def unwrapped: ClassInfo = {
    def unwrap(e: ClassInfo): ClassInfo = {
      if (e.isCollection || e.isOption || e.isKnowable) {
        // there are concrete collection types that have no type args
        // e.g. the Range class, hence we need to use headOption
        e.typeArgs.headOption map { unwrap } getOrElse { e }
      } else {
        e
      }
    }
    unwrap(this)
  }

  final def isSuperTypeOf(other: ClassInfo): Boolean = {
    other.selfAndParents.exists(p => p == this)
  }

  def annotations: List[AnnotationInfo] = classSymbol.annotations.map { AnnotationInfo(_) }
  def classSymbol: ClassSymbol
  def referrers(implicit f: ClassInfo => Boolean = _ => true): Iterable[ClassInfo] = {
    ClassInfo.incomingGraph.flatMap { case (k, rs) =>
      if (f(k) && k.isSuperTypeOf(this)) {
        rs.filter(f)
      } else {
        Nil
      }
    }
  }

  def piiElements: Seq[PIIDetails]
}

final case class TypeClassInfo private (t: Type) extends ClassInfo {

  override def equals(o: Any): Boolean = {
    o match {
      case other: TypeClassInfo => t =:= other.t
      case _                    => false
    }
  }

  override def hashCode(): Int = {
    fullName.hashCode()
  }

  override def isKnownNonStorable: Boolean = !(isEmbeddable ||
    (isEntity && !annotations.exists(_.isType(typeOf[stored]))) ||
    isEvent)

  override def isTrait: Boolean = t.typeSymbol.isClass && t.typeSymbol.asClass.isTrait
  override def isAbstract: Boolean = t.typeSymbol.isAbstract && !isEnumeration
  override def isObject: Boolean = t.typeSymbol.isModuleClass
  override def isEvent: Boolean = annotations.exists(_.isType(typeOf[event]))
  override def isEmbeddable: Boolean = annotations.exists(_.isType(typeOf[embeddable]))
  override def isEntity: Boolean = annotations.exists(_.isType(typeOf[entity]))
  override def isMeta: Boolean = annotations.exists(_.isType(typeOf[meta]))
  override def isOption: Boolean = t <:< typeOf[Option[_]]
  override def isKnowable: Boolean = t <:< typeOf[Knowable[_]]
  override def isCollection: Boolean = t <:< typeOf[Iterable[_]]
  override def isEnumeration: Boolean = t <:< typeOf[Enumeration#Value] || isJavaEnumeration
  override def isJavaEnumeration: Boolean = t.typeSymbol.isJavaEnum
  override def isMap: Boolean = t <:< typeOf[Map[_, _]]
  override def typeArgs: List[ClassInfo] =
    t.typeArgs.map { ClassInfo(_) }
  override def className: String = ReflectionHelper.typeString(t, fqn = true, typeArgs = false)
  override def fullName: String = {
    ReflectionHelper.typeString(t, fqn = true, typeArgs = true)
  }
  private def metaDataClassInfo: Option[ClassInfo] = {
    if (isEntity || isEmbeddable || isEvent) {
      val name = ReflectionHelper.typeString(t, fqn = true, typeArgs = false)
      ClassInfo.optimusClassesByClassName.get(name)
    } else {
      None
    }
  }
  override def fields(includeTypeArgs: Boolean = true): Iterable[FieldInfo] = {
    if (isJavaEnumeration) {
      // Cleanest way to get java enum members seems to be through
      // java reflection.
      val javaCls = currentMirror.runtimeClass(t)
      javaCls.getEnumConstants.map { m =>
        EnumerationFieldInfo(m.toString, this)
      }
    } else if (isEnumeration) {
      // Scala Enumeration
      def tryEnumerateFields(tpe: Type): Iterable[EnumerationFieldInfo] = tpe.dealias match {
        case TypeRef(pre, _, _) if pre.termSymbol.isModule =>
          val module = pre.termSymbol.asModule
          val inst = currentMirror.reflectModule(module).instance.asInstanceOf[Enumeration]
          inst.values.unsorted.map { v =>
            EnumerationFieldInfo(v.toString, this)
          }
        case RefinedType(parents, _) =>
          parents.flatMap { tryEnumerateFields }
      }
      tryEnumerateFields(t)
    } else if (isObject) {
      Nil
    } else {
      val syms = if (t.typeSymbol.asClass.isCaseClass) {
        // A bit tricky to get case class fields. We have to make sure
        // a) we don't grab any non-ctor vals
        // b) we don't want to lose type info
        // c) we don't want to drop any annotations
        // Going with classSymbol loses the actual type args and drops the annotations
        // Going by type.members gives us non-ctor vals
        // So we'll get the names of ctor params from the classSymbol and then
        // select matching members that are getters.
        t.typeSymbol.asClass.primaryConstructor.typeSignature.paramLists.headOption
          .map { ctorArgs =>
            t.members.filter { m =>
              m.isMethod && m.asMethod.isGetter && ctorArgs.exists { _.fullName == m.fullName }
            }
          }
          .getOrElse(Seq.empty)
      } else {
        t.members.filter((s: Symbol) => s.isTerm && !s.isMethod && !s.isStatic)
      }
      syms.map { f =>
        SymbolFieldInfo(f.asTerm, t, includeTypeArgs)
      }
    }
  }

  override def localName(showTypeArgs: Boolean = false): String = {
    ReflectionHelper.typeString(t, fqn = false, showTypeArgs)
  }

  override def children(implicit f: ClassInfo => Boolean = _ => true): Iterable[ClassInfo] = {
    metaDataClassInfo.map { _.children(f) } getOrElse {
      // If this isn't an @entity/@embeddable/@event, the best we can do is directChildren
      directChildren(f)
    }
  }

  override def directChildren(implicit f: ClassInfo => Boolean = _ => true): Iterable[ClassInfo] = {
    // Implementing this for any arbitrary type would require walking the full classpath and be very slow
    // But we can special case classes that are @entity/@embeddable/@event at the expense of losing
    // any type arguments. Here we lookup the corresponding MetaDataClassInfo and use it to get the
    // children.
    metaDataClassInfo.map { _.directChildren(f) } getOrElse {
      // For non-optimus classes, we can still implement this reasonably for...
      if (!isEnumeration && t.typeSymbol.isClass) {
        val clsSym = t.typeSymbol.asClass
        // ... sealed classes as their subclasses are known:
        if (clsSym.isSealed) {
          clsSym.knownDirectSubclasses.map { a => ClassInfo(a.asClass.toType) }
        } else if (!clsSym.isFinal) {
          // This is the problem case: We can't walk the full classpath and find every child of an arbitrary
          // class so we'll just have to throw.
          throw new IllegalArgumentException(s"Non-final and non-sealed class ${clsSym.fullName} encountered")
        } else {
          // ... for final classes, there are no children
          Iterable.empty[ClassInfo]
        }
      } else {
        // For enums, there a no children
        Iterable.empty[ClassInfo]
      }
    }
  }

  override def directParents(implicit f: ClassInfo => Boolean = _ => true): Iterable[ClassInfo] = {
    (t.finalResultType match {
      case c: ClassInfoTypeApi => c.parents
      case _                   => List.empty[Type]
    }).map { ClassInfo(_) }.filter(f)
  }

  override def parents(implicit f: ClassInfo => Boolean = _ => true): Iterable[ClassInfo] = {
    if (isEnumeration) {
      Seq(this)
    } else {
      t.baseClasses
        .filter(sym =>
          !(sym == definitions.AnyClass
            || sym == definitions.AnyRefClass
            || sym == definitions.AnyValClass
            || (sym match {
              case c: TypeSymbol => c.asType != t
              case _             => false
            })))
        .map { c =>
          ClassInfo(c.asType.toType)
        }
    }
  }

  override def selfAndParents(implicit f: ClassInfo => Boolean = _ => true): Iterable[ClassInfo] = {
    parents(f).toSeq :+ this
  }

  override def classSymbol: ClassSymbol = t.typeSymbol.asClass

  override val piiElements: Seq[PIIDetails] = DatatypeUtil.extractPiiElementsList(t.getClass.getDeclaredFields)
}

final case class MetaDataClassInfo(meta: ClassMetaData, classLoader: ClassLoader) extends ClassInfo {
  override def isKnownNonStorable: Boolean = false // No metadata for non-stored so we know we are storeable
  override def isTrait: Boolean = meta.isTrait
  override def isAbstract: Boolean = meta.isAbstract
  override def isObject: Boolean = meta.isObject
  override def isEvent: Boolean = meta.isEvent
  override def isEmbeddable: Boolean = meta.isEmbeddable
  override def isEntity: Boolean = meta.isEntity
  override def isMeta: Boolean = meta.isMeta
  override def isEnumeration: Boolean = false
  override def isJavaEnumeration: Boolean = false
  override def isOption: Boolean = false
  override def isKnowable: Boolean = false
  override def isCollection: Boolean = false
  override def isMap: Boolean = false
  override def localName(showTypeArgs: Boolean): String = {
    ReflectionHelper.typeString(tpe, fqn = false, showTypeArgs)
  }
  override def directParents(implicit f: ClassInfo => Boolean): Iterable[ClassInfo] =
    meta.parents.map { MetaDataClassInfo(_, classLoader) }.filter(f)

  override def parents(implicit f: ClassInfo => Boolean): Iterable[ClassInfo] =
    meta.allParents.map { MetaDataClassInfo(_, classLoader) }.filter(f)

  override def selfAndParents(implicit f: ClassInfo => Boolean): Iterable[ClassInfo] = parents.toSeq :+ this

  override def directChildren(implicit f: ClassInfo => Boolean = _ => true): Iterable[ClassInfo] =
    meta.children.map { MetaDataClassInfo(_, classLoader) }.filter(f)

  override def children(implicit f: ClassInfo => Boolean = _ => true): Iterable[ClassInfo] =
    meta.allChildren.map { MetaDataClassInfo(_, classLoader) }.filter(f)

  override lazy val classSymbol: ClassSymbol =
    ReflectionHelper.getClassSymbolFromName(meta.fullClassName, classLoader)
  override def fields(includeTypeArgs: Boolean = true): Iterable[FieldInfo] = {
    val syms = if (isEmbeddable) {
      // A bit tricky to get case class fields. We have to make sure
      // a) we don't grab any non-ctor vals
      // b) we don't want to drop any annotations
      // Going with primaryConstructor loses the the annotations
      // Going by type.members gives us non-ctor vals
      // So we'll get the names of ctor params from the primaryctor and then
      // select matching members that are getters.
      classSymbol.primaryConstructor.typeSignature.paramLists.headOption
        .map { ctorArgs =>
          classSymbol.typeSignature.members.filter { m =>
            m.isMethod && m.asMethod.isGetter && ctorArgs.exists { _.fullName == m.fullName }
          }
        }
        .getOrElse(Seq.empty)

    } else {
      classSymbol.typeSignature.members.filter(if (isEntity) { (s: Symbol) =>
        !s.isStatic &&
        (s match {
          case t: TermSymbol =>
            t.annotations.exists { a =>
              a.tree.tpe =:= typeOf[key] || a.tree.tpe =:= typeOf[valAccessor]
            }
          case _ =>
            // Treat @key/@indexed/@projected defs as fields as they are important
            // and persisted to DAL.
            s.annotations.exists { a =>
              a.tree.tpe =:= typeOf[key] || a.tree.tpe =:= typeOf[indexed] || a.tree.tpe =:= typeOf[projected]
            }
        })
      } else (s: Symbol) => s.isTerm && !s.isMethod)
    }
    syms.map { f =>
      SymbolFieldInfo(f.asTerm, tpe, includeTypeArgs)
    }
  }

  val catalogingInfo: Option[MetaAnnotationAttributes] = meta.catalogingInfo

  override val piiElements: Seq[PIIDetails] = meta.piiElements

  private lazy val tpe =
    RuntimeMirror.forClassLoader(classLoader).classSymbol(Class.forName(meta.fullClassName, false, classLoader)).toType

  override def className: String = classSymbol.fullName
  override def fullName: String = {
    ReflectionHelper.typeString(tpe, fqn = true, typeArgs = true)
  }

  override def typeArgs: List[ClassInfo] = List.empty[ClassInfo]
}

object ClassInfo extends ClassInfoBase {
  protected val resourceFinder: ResourceFinder = CurrentClasspathResourceFinder
  protected val classLoader: ClassLoader = ClassLoaderHook.currentClassLoader
}

class ClassInfoFromClassLoader(
    protected val resourceFinder: ResourceFinder,
    val classLoader: ClassLoader
) extends ClassInfoBase

trait ClassInfoBase extends Log {
  private val excludedPackages: Set[String] = Set("scala.", "java.", "msjava.", "optimus.")
  private val packageExceptions: Set[String] = Set(
    "optimus.stargazer.",
    "optimus.platform.util.dalschema.test." // Package for lasses that are used in tests so need to be supported
  )
  protected def resourceFinder: ResourceFinder
  protected def classLoader: ClassLoader

  def isExcludedByPackage(e: ClassInfo): Boolean = {
    !packageExceptions.exists { e.fullName.startsWith } && excludedPackages.exists { e.fullName.startsWith }
  }

  lazy val incomingGraph: Map[ClassInfo, Seq[ClassInfo]] = buildIncomingGraph(includeInheritance = true)
  lazy val incomingGraphNoInheritance: Map[ClassInfo, Seq[ClassInfo]] = buildIncomingGraph(includeInheritance = false)

  // Referring class to referrer map. The matching ignores type args such that
  // Any Foo[X] is considered to be a reference to type Foo[T]
  private def buildIncomingGraph(includeInheritance: Boolean): Map[ClassInfo, Seq[ClassInfo]] = {
    log.info("Generating incomingGraph")
    def addEdge(to: ClassInfo, from: ClassInfo, m: mutable.Map[ClassInfo, Seq[ClassInfo]]) = {
      val col = m.getOrElse(to, Seq.empty) :+ from
      m += (to -> col)
    }

    val (time, res) = AdvancedUtils.timed {
      (allStoredEntities ++ allEmbeddables ++ allEvents).foldLeft(mutable.Map.empty[ClassInfo, Seq[ClassInfo]]) {
        case (m, c) =>
          if (includeInheritance) {
            // Add edges from derived type to their direct base types
            c.directParents map { p =>
              addEdge(p, c, m)
            }
          }
          // Add edges from using type to types used in each of its fields
          c.fields(includeTypeArgs = false) foreach { _.association().allReferences.map { addEdge(_, c, m) } }
          m
      }
    }
    log.info(s"Generated incomingGraph for ${res.size} classes in ${time / 1e9} seconds")
    res.toMap
  }

  lazy val allStoredEntities: Set[ClassInfo] = {
    log.info("Loading stored entity metadata")
    val (time, res) = AdvancedUtils.timed {
      new StoredHierarchyManager(resourceFinder, classLoader).metaData.map { case (_, e) =>
        forMetaData(e)
      }.toSet
    }
    log.info(s"Loaded metadata for ${res.size} stored entities in ${time / 1e9} seconds")
    res
  }

  lazy val allEntities: Set[ClassInfo] = {
    log.info("Loading entity metadata")
    val (time, res) = AdvancedUtils.timed {
      new EntityHierarchyManager(resourceFinder, classLoader).metaData.map { case (_, e) =>
        forMetaData(e)
      }.toSet
    }
    log.info(s"Loaded metadata for ${res.size} entities in ${time / 1e9} seconds")
    res
  }

  lazy val optimusClassesByClassName: Map[String, ClassInfo] = {
    (allStoredEntities ++ allEmbeddables ++ allEvents).map { e =>
      e.className -> e
    }.toMap
  }

  def normalizeClassname(classname: String): String =
    classname
      .stripSuffix("$") // deals with @store @entity objects
      .replaceAll("\\.package\\$", ".")
      .replaceAll("\\$", ".")
      .replaceAll("###", "")
  lazy val allMetaDatas: Map[String, ClassMetaData] = {
    log.info("Loading @meta metadata")
    val (time, res) = AdvancedUtils.timed {
      new MetaHierarchyManager(resourceFinder, classLoader).metaData
    }
    log.info(s"Loaded metadata for ${res.size} @meta in ${time / 1e9} seconds")

    res.map { case (name, clsMeta) =>
      (normalizeClassname(name), clsMeta)
    }
  }

  lazy val allEmbeddables: Set[ClassInfo] = {
    log.info("Loading embeddable metadata")
    val (time, res) = AdvancedUtils.timed {
      new EmbeddableHierarchyManager(resourceFinder, classLoader).metaData.map { case (_, e) =>
        forMetaData(e)
      }.toSet
    }
    log.info(s"Loaded metadata for ${res.size} embeddables in ${time / 1e9} seconds")
    res
  }

  lazy val allEvents: Set[ClassInfo] = {
    log.info("Loading event metadata")
    val (time, res) = AdvancedUtils.timed {
      new EventHierarchyManager(resourceFinder, classLoader).metaData.map { case (_, e) =>
        forMetaData(e)
      }.toSet
    }
    log.info(s"Loaded metadata for ${res.size} events in ${time / 1e9} seconds")
    res
  }

  def fromName(fqn: String): ClassInfo = {
    optimusClassesByClassName.getOrElse(
      fqn,
      TypeClassInfo(ReflectionHelper.getClassSymbolFromName(fqn, classLoader).toType)
    )
  }

  def fromNameOpt(fqn: String): Option[ClassInfo] = Try { fromName(fqn) }.toOption

  def apply(t: Type, includeTypeArgs: Boolean = true): ClassInfo = {
    val tpe = t.dealias
    tpe match {
      case n: Type @unchecked if n <:< typeOf[optimus.graph.Node[_]] =>
        apply(n.typeArgs.head)
      case _ =>
        optimusClassesByClassName.getOrElse(
          ReflectionHelper.typeString(tpe, fqn = true, typeArgs = includeTypeArgs),
          if (tpe.typeSymbol.isClass) {
            TypeClassInfo(tpe)
          } else {
            tpe.typeSymbol.info match {
              case tb: TypeBounds => apply(tb.hi)
              case _              => throw new IllegalArgumentException(s"Can't create TypeClassInfo for ${t.toString}")
            }
          }
        )
    }
  }

  def forMetaData(c: ClassMetaData): ClassInfo = {
    MetaDataClassInfo(c, classLoader)
  }
}
