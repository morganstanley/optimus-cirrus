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
package optimus.buildtool.config

import optimus.buildtool.config.MetaBundle.toMavenCompatibleStr

import scala.collection.compat._
import com.github.plokhotnyuk.jsoniter_scala.core._
import scala.util.hashing.MurmurHash3

trait Id {
  def contains(scopeId: ScopeId): Boolean
  def elements: Seq[String]
  // For a.b.c.d returns Seq("", a, a.b, a.b.c)
  def parents: Seq[ParentId] = elements.inits.to(Seq).tail.map(ParentId.parse).reverse
  def withParents: Seq[Id] = parents :+ this
  def properPath: String = elements.mkString(".")
  override def toString: String = properPath
}
object Id {
  def parse(elements: Seq[String]): Id = elements match {
    case Seq(meta, bundle, module, tpe) => ScopeId(meta, bundle, module, tpe)
    case Seq(meta, bundle, module)      => ModuleId(meta, bundle, module)
    case Seq(meta, bundle)              => MetaBundle(meta, bundle)
    case Seq(meta)                      => MetaId(meta)
    case Nil                            => WorkspaceId
    case _                              => throw new RuntimeException(s"Invalid id: ${elements.mkString(".")}")
  }
  def parse(s: String): Id = parse(s.split('.').to(Seq))
}

sealed trait ParentId extends Id { // Not a ScopeId
  override def withParents: Seq[ParentId] = parents :+ this
}
object ParentId {
  def parse(elements: Seq[String]): ParentId = Id.parse(elements) match {
    case p: ParentId => p
    case _           => throw new RuntimeException(s"Invalid parent id: ${elements.mkString(".")}")
  }
}

sealed trait HasMetaBundle extends Id {
  def metaBundle: MetaBundle
}

sealed trait HasModuleId extends HasMetaBundle {
  def fullModule: ModuleId
}

/**
 * Represents a compilation scope (i.e. a set of sources that are compiled together).
 *
 * @param meta
 *   The meta-bundle e.g. "optimus"
 * @param bundle
 *   The bundle e.g. buildtool or "platform"
 * @param module
 *   The module e.g. "core"
 * @param tpe
 *   The source type, e.g. "main", "test" or "uiTest"
 */
final case class ScopeId(meta: String, bundle: String, module: String, tpe: String) extends HasModuleId {
  def forMavenRelease: ScopeId =
    new ScopeId(toMavenCompatibleStr(meta), toMavenCompatibleStr(bundle), toMavenCompatibleStr(module), tpe)
  def tuple: (String, String, String, String) = (meta, bundle, module, tpe)
  override def elements: Seq[String] = Seq(meta, bundle, module, tpe)
  def isMain: Boolean = tpe == "main"
  def isTest: Boolean = tpe.toLowerCase contains "test"
  def isRoot: Boolean = this == ScopeId.RootScopeId
  def metaBundle: MetaBundle = MetaBundle(meta, bundle)
  override def fullModule: ModuleId = ModuleId(meta, bundle, module)
  override def contains(scopeId: ScopeId): Boolean = this == scopeId
  override def toString: String =
    if (Seq(meta, bundle, module, tpe).forall(_.isEmpty)) "." else super.toString

  @transient
  override val hashCode: Int = MurmurHash3.productHash(this)
}

object ScopeId {
  implicit val ordering: Ordering[ScopeId] = Ordering.by(s => (s.meta, s.bundle, s.module, s.tpe))

  val RootScopeId: ScopeId = ScopeId("", "", "", "")

  implicit val ScopeIdJsonValueCodec: JsonValueCodec[ScopeId] = new JsonValueCodec[ScopeId] {
    override def decodeValue(in: JsonReader, default: ScopeId): ScopeId = ScopeId.parse(in.readString(null))
    override def encodeValue(x: ScopeId, out: JsonWriter): Unit = out.writeVal(x.toString)
    override def nullValue: ScopeId = null
  }

  def parse(str: String): ScopeId = str match {
    case "." | ""               => RootScopeId
    case ScopeIdString(scopeId) => scopeId
    case _ =>
      throw new IllegalArgumentException(
        s"Expected a string of form '<meta>.<bundle>.<module>.<type>' but found '$str'")
  }
  // Note: This method never returns Some(RootScopeId)
  def parseOpt(str: String): Option[ScopeId] = str match {
    case ScopeIdString(scopeId) => Some(scopeId)
    case _                      => None
  }

  def apply(moduleId: ModuleId, tpe: String): ScopeId = {
    ScopeId(moduleId.meta, moduleId.bundle, moduleId.module, tpe)
  }
}

/** An extractor for ScopeIds from Strings */
object ScopeIdString {
  private[config] val ScopeIdFormat = """([\w\-]*)\.([\w\-]*)\.([\w\-]*)\.([\w\-]*)""".r

  def unapply(str: String): Option[ScopeId] = str match {
    case ScopeIdFormat(meta, bundle, module, tpe) => Some(ScopeId(meta, bundle, module, tpe))
    case _                                        => None
  }
}

final case class PartialScopeId(
    meta: Option[String],
    bundle: Option[String],
    module: Option[String],
    tpe: Option[String]) {

  /**
   * Returns true if all elements of this partial scope id either match other or are None.
   */
  def contains(other: ScopeId): Boolean = {
    def matches(left: Option[String], right: String) = left.forall(_ == right)

    matches(meta, other.meta) &
      matches(bundle, other.bundle) &
      matches(module, other.module) &
      matches(tpe, other.tpe)
  }

  override def toString: String =
    s"${meta.getOrElse("*")}.${bundle.getOrElse("*")}.${module.getOrElse("*")}.${tpe.getOrElse("*")}"
}

object PartialScopeId {
  val RootPartialScopeId: PartialScopeId = PartialScopeId(Some(""), Some(""), Some(""), Some(""))

  def parse(str: String): PartialScopeId = str match {
    case "." | ""                      => RootPartialScopeId
    case PartialScopeIdString(scopeId) => scopeId
    case _ =>
      throw new IllegalArgumentException(
        s"Expected a string of form '<meta>.<bundle>.<module>.<type>' but found '$str'")
  }
}

/** An extractor for PartialScopeIds from Strings */
object PartialScopeIdString {
  private def opt(s: String): Option[String] = Some(s).filter(_.nonEmpty)

  def unapply(str: String): Option[PartialScopeId] = str match {
    case ScopeIdString.ScopeIdFormat(meta, bundle, module, tpe) =>
      Some(PartialScopeId(opt(meta), opt(bundle), opt(module), opt(tpe)))
    case _ => None
  }
}

object RelaxedIdString {
  def parts(str: String): Seq[Option[String]] = {
    val partsSeq = split(str)
    val init = (0 to 2).map(i => partsSeq.lift(i).filter(_.nonEmpty))
    // Note that we take all remaining elements here, even if there's more than one. ModuleScopedNames, for example,
    // can have a final element (the name) that includes ".".
    val last = Some(partsSeq.drop(3).mkString(".")).filter(_.nonEmpty)
    init :+ last
  }

  // Note the `-1` limit here, so ensure we can have trailing empty Strings
  private[config] def split(str: String): Seq[String] = str.stripPrefix(":").split("[:/.]", -1).to(Seq)
}

/** A tolerant extractor for ScopeId from a String */
object RelaxedScopeIdString {
  def unapply(str: String): Option[ScopeId] = RelaxedIdString.split(str) match {
    case Seq(meta, bundle, module, tpe) => Some(ScopeId(meta, bundle, module, tpe))
    case _                              => None
  }

  def asPartial(str: String): PartialScopeId = {
    val partsSeq = RelaxedIdString.split(str)
    val Seq(meta, bundle, module, tpe) = (0 to 3).map(i => partsSeq.lift(i).filter(_.nonEmpty))
    PartialScopeId(meta, bundle, module, tpe)
  }
}

trait HasScopeId {
  def id: ScopeId
}

final case class ModuleId(meta: String, bundle: String, module: String) extends ParentId with HasModuleId {
  def metaBundle: MetaBundle = MetaBundle(meta, bundle)
  def scope(tpe: String): ScopeId = ScopeId(meta, bundle, module, tpe)
  override def elements: Seq[String] = Seq(meta, bundle, module)
  override def contains(scopeId: ScopeId): Boolean = this == scopeId.fullModule
  override def fullModule: ModuleId = this
}

object ModuleId {
  private val ModuleIdFormat = """([\w\-]*)\.([\w\-]*)\.([\w\-]*)""".r

  def apply(scopeId: ScopeId): ModuleId = apply(scopeId.meta, scopeId.bundle, scopeId.module)

  def parse(str: String): ModuleId = str match {
    case ModuleIdFormat(meta, bundle, module) => ModuleId(meta, bundle, module)
    case _ =>
      throw new IllegalArgumentException(s"Expected a string of form '<meta>.<bundle>.<module>' but found '$str'")
  }
  def parse(elements: Seq[String]): ModuleId = Id.parse(elements) match {
    case m: ModuleId => m
    case _           => throw new RuntimeException(s"Invalid module id: ${elements.mkString(".")}")
  }
  def parseOpt(str: String): Option[ModuleId] = str match {
    case ModuleIdFormat(meta, bundle, module) => Some(ModuleId(meta, bundle, module))
    case _                                    => None
  }
}

final case class MetaBundle(meta: String, bundle: String) extends ParentId with HasMetaBundle {
  def forMavenRelease(): MetaBundle = new MetaBundle(toMavenCompatibleStr(meta), toMavenCompatibleStr(bundle))
  def fullMeta: MetaId = MetaId(meta)
  def module(name: String): ModuleId = ModuleId(meta, bundle, name)
  override def elements: Seq[String] = Seq(meta, bundle)
  override def contains(scopeId: ScopeId): Boolean = this == scopeId.fullModule.metaBundle
  override def metaBundle: MetaBundle = this
  def isEmpty: Boolean = meta.isEmpty && bundle.isEmpty
}

object MetaBundle {

  def toMavenCompatibleStr(s: String): String = s.replaceAll("_", "-").toLowerCase

  implicit val ord: Ordering[MetaBundle] = Ordering.by(mb => (mb.meta, mb.bundle))

  def parse(str: String): MetaBundle = str.split("\\.") match {
    case Array(meta, bundle) =>
      MetaBundle(meta, bundle)
    case _ =>
      throw new IllegalArgumentException(s"Expected a string of form '<meta>.<bundle>' but found '$str'")
  }

  def apply(meta: String, bundle: String): MetaBundle =
    new MetaBundle(meta, bundle)

}

final case class MetaId(meta: String) extends ParentId {
  def bundle(name: String): MetaBundle = MetaBundle(meta, name)
  override def elements: Seq[String] = Seq(meta)
  override def contains(scopeId: ScopeId): Boolean = this == scopeId.fullModule.metaBundle.fullMeta
}

object WorkspaceId extends ParentId {
  override def elements: Seq[String] = Nil
  override def contains(scopeId: ScopeId): Boolean = true
}
