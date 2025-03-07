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
package optimus.buildtool.format

import java.nio.file.Path
import java.nio.file.Paths
import com.typesafe.config.Config
import com.typesafe.config.ConfigException.BadValue
import com.typesafe.config.ConfigList
import com.typesafe.config.ConfigObject
import com.typesafe.config.ConfigValue
import com.typesafe.config.ConfigValueType
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.OctalMode
import optimus.buildtool.config.PartialScopeId
import optimus.buildtool.config.RelaxedIdString
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.RelativePath

import scala.collection.compat._
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.collection.immutable.Seq

object ConfigUtils {

  implicit class ConfOps(conf: Config) {
    private def asObject(
        origin: ObtFile
    )(entry: (String, ConfigValue)): Result[(String, Config)] =
      entry match {
        case (key, obj: ConfigObject) =>
          Success(key -> obj.toConfig)
        case (key, other) =>
          val msg = s"Expected object but got ${other.valueType} for key $key"
          origin.errorAt(other, msg).failure
      }

    def configEntries(origin: ObtFile): Result[Seq[(String, ConfigValue)]] =
      Result.tryWith(origin, conf) {
        Success(conf.root().entrySet().asScala.map(e => (e.getKey, e.getValue)).to(Seq))
      }

    def configEntries(origin: ObtFile, depth: Int): Result[Seq[(String, ConfigValue)]] = {
      def join(prefix: String, key: String) = if (prefix.isEmpty) key else s"$prefix.$key"

      def inner(cfg: Config, prefix: String, remaining: Int): Result[Seq[(String, ConfigValue)]] =
        if (remaining == 1)
          Success(cfg.root().entrySet().asScala.map(e => (join(prefix, e.getKey), e.getValue)).to(Seq))
        else
          ResultSeq
            .traverse(cfg.root().entrySet().asScala.to(Seq)) { e =>
              ResultSeq(asObject(origin)((e.getKey, e.getValue)).flatMap { case (k, v) =>
                inner(v, join(prefix, k), remaining - 1)
              })
            }
            .value

      inner(conf, "", depth)
    }

    def resolveWithReferences(other: Config): Config = {
      // Why are we not using resolveWith()? Because it doesn't always work with optional overrides
      // see https://github.com/lightbend/config/issues/332#issuecomment-127078621
      val extraKeys = other.keySet -- conf.keySet
      val fullConfig = conf.withFallback(other).resolve()
      extraKeys.foldLeft(fullConfig) { (c, key) =>
        c.withoutPath(key)
      }
    }

    def keySet: Set[String] = conf.root().entrySet().asScala.map(_.getKey).to(Set)

    def nestedWithFilter(origin: ObtFile)(filter: ((String, ConfigValue)) => Boolean): Result[Seq[(String, Config)]] =
      Result.traverseWithFilter(configEntries(origin))(asObject(origin))(filter)

    def nested(origin: ObtFile): Result[Seq[(String, Config)]] =
      Result.traverse(configEntries(origin))(asObject(origin))

    def nested(origin: ObtFile, depth: Int): Result[Seq[(String, Config)]] =
      Result.traverse(configEntries(origin, depth))(asObject(origin))

    def keys(origin: ObtFile): Result[Seq[String]] = configEntries(origin).map(_.map(_._1))

    def values(key: String): Seq[ConfigValue] = conf.getList(key).asScala.to(Seq)

    def configs(key: String): Seq[Config] = conf.getConfigList(key).asScala.to(Seq)

    def seqOrEmpty(key: String): Seq[String] = arrayOrDefaults(key, Array.empty).to(Seq)

    def setOrEmpty(key: String): Set[String] = arrayOrDefaults(key, Array.empty).to(Set)

    def arrayOrDefaults(key: String, default: Array[String]): Array[String] =
      if (conf.hasPath(key)) conf.getStringList(key).asScala.toArray else default

    def nestedKeyConfigOrEmpty(origin: ObtFile, config: Config, key: String): Seq[Config] =
      config.optionalConfigList(key) match {
        case Some(configs) => configs
        case None =>
          val nestedConfig = config.nested(origin).getOrElse(Seq.empty)
          nestedConfig.flatMap { case (name, conf) => nestedKeyConfigOrEmpty(origin, conf, key) }
      }

    def checkEmptyProperties(origin: ObtFile, expected: Keys.KeySet): Seq[Message] = {
      val validKeys = expected.all -- keys(origin).getOrElse(Nil)
      if (validKeys == expected.all) {
        Seq(
          origin.warningAt(
            v = conf.root(),
            msg = s"No valid keys found. Expected at least one of ${expected.order.mkString(", ")}"
          )
        )
      } else Seq.empty
    }

    def checkExtraProperties(
        origin: ObtFile,
        expected: Keys.KeySet,
        filter: String => Boolean = _ => true
    ): Seq[Message] = {
      val extraKeys = keys(origin).getOrElse(Nil).toSet.filter(filter) -- expected.all -- Keys.ignoredKeysForValidation
      def msg(key: String) = s"Unrecognized key: '$key', possible ones: ${expected.order.mkString(", ")}"
      extraKeys.map(k => origin.warningAt(conf.getValue(k), msg(k))).to(Seq)
    }

    def checkUniqueValuesForKey(origin: ObtFile, topLevelKey: String, key: String): Seq[Message] = {
      // Checks if all values for a given key within a Config are unique
      val topLevelConfig = conf.configs(topLevelKey)
      val seq = topLevelConfig.flatMap(cfg =>
        cfg
          .optionalString(key)
          .map(x => x.strip()))

      if (seq.distinct != seq) {
        val diff = (seq diff seq.distinct).distinct
        Seq(
          origin.errorAt(
            v = conf.getValue(topLevelKey),
            msg = s"Values for '$key' in $topLevelKey must be unique. Duplicated value(s): ${diff.mkString(" , ")}."
          ))
      } else Seq.empty
    }

    def nonBlankString(origin: ObtFile, key: String): Result[String] = {
      val nonBlankValue = conf.optionalString(key).flatMap { value =>
        if (value.isBlank) None else Some(value)
      }

      nonBlankValue match {
        case Some(value) => Success(value)
        case None =>
          origin.failure(
            v = conf.root(),
            msg = s"Invalid value: '$key' must not be blank"
          )
      }
    }

    def loadOctal(origin: ObtFile, key: String): Result[OctalMode] =
      Result.tryWith(origin, conf.getValue(key)) {
        try {
          Success(OctalMode.fromModeString(conf.getString(key)))
        } catch {
          case NonFatal(t) => throw new BadValue(key, t.getMessage, t)
        }
      }

    def checkExclusiveProperties(origin: ObtFile, exclusiveKeys: Keys.KeySet): Seq[Message] = {
      val repeatedKeys = keys(origin).getOrElse(Nil).toSet.intersect(exclusiveKeys.all)

      def msg(key: String) = s"Invalid key $key: pick only one between ${exclusiveKeys.order.mkString(", ")} "

      if (repeatedKeys.size > 1)
        repeatedKeys.map(k => origin.errorAt(conf.getValue(k), msg(k))).to(Seq)
      else Seq.empty
    }

    def optionalString(path: String): Option[String] = if (conf.hasPath(path)) Some(conf.getString(path)) else None

    def optionalStringList(path: String): Option[Seq[String]] =
      if (conf.hasPath(path)) Some(conf.getStringList(path).asScala.to(Seq)) else None

    def stringListOrEmpty(path: String): Seq[String] = optionalStringList(path) getOrElse Nil

    def stringConfigListOrEmpty(path: String): Seq[(String, ConfigValue)] =
      if (conf.hasPath(path)) conf.getStringList(path).asScala.to(Seq) zip conf.getList(path).asScala.to(Seq)
      else Nil

    def optionalBoolean(path: String): Option[Boolean] = if (conf.hasPath(path)) Some(conf.getBoolean(path)) else None

    def optionalValue(path: String): Option[ConfigValue] = if (conf.hasPath(path)) Some(conf.getValue(path)) else None

    def optionalConfig(path: String): Option[Config] = if (conf.hasPath(path)) Some(conf.getConfig(path)) else None

    def optionalConfigList(path: String): Option[Seq[Config]] =
      if (conf.hasPath(path)) Some(conf.getConfigList(path).asScala.to(Seq)) else None

    def optionalInt(path: String): Option[Int] = if (conf.hasPath(path)) Some(conf.getInt(path)) else None

    def intOrDefault(path: String, default: Int): Int =
      if (conf.hasPath(path)) conf.getInt(path) else default

    def stringOrDefault(path: String, default: String): String =
      if (conf.hasPath(path)) conf.getString(path) else default

    def booleanOrDefault(path: String, default: Boolean): Boolean =
      if (conf.hasPath(path)) conf.getBoolean(path) else default

    def stringMapOrEmpty(path: String, file: ObtFile): Result[Map[String, String]] =
      if (!conf.hasPath(path)) Success(Map.empty)
      else {
        def valueToString(value: ConfigValue): Result[String] = value.valueType() match {
          case ConfigValueType.OBJECT | ConfigValueType.LIST =>
            file.errorAt(value, "Expect only string values in manifest").failure
          case _ =>
            Success(value.unwrapped().toString)
        }

        Result
          .traverse(conf.getObject(path).toConfig.configEntries(file)) { case (key, value) =>
            valueToString(value).map(key -> _)
          }
          .map(_.toMap)
      }

    def stringListMapOrEmpty(path: String, file: ObtFile): Result[Map[String, Seq[String]]] =
      if (!conf.hasPath(path)) Success(Map.empty)
      else {
        Result
          .traverse(conf.getObject(path).toConfig.configEntries(file)) { case (key, value) =>
            valueToStrings(value, file).map(key -> _)
          }
          .map(_.toMap)
      }

    private def valueToStrings(value: ConfigValue, file: ObtFile): Result[Seq[String]] = value.valueType() match {
      case ConfigValueType.LIST =>
        Success(value.asInstanceOf[ConfigList].unwrapped().asScala.to(Seq).map(_.toString))
      case ConfigValueType.OBJECT | ConfigValueType.NULL =>
        file.errorAt(value, "Expect only list of string values in manifest").failure
      case _ =>
        Success(Seq(value.unwrapped().toString))
    }

    def relativePath(key: String, file: ObtFile): Result[RelativePath] =
      Result.tryWith(file, conf.getValue(key)) {
        val rawPath = conf.getString(key)
        try {
          val path = Paths.get(rawPath)
          if (path.startsWith("..")) {
            val msg = s"Invalid path $rawPath: referring to directories above is forbidden"
            file.failure(conf.getValue(key), msg)
          } else if (path.isAbsolute) {
            val msg = s"Invalid path $path: you cannot use an absolute path"
            file.failure(conf.getValue(key), msg)
          } else {
            Success(RelativePath(path))
          }

        } catch {
          case NonFatal(t) => file.failure(conf.getValue(key), s"Invalid path $rawPath: ${t.getMessage}")
        }
      }

    def directory(key: String, file: ObtFile): Result[Directory] =
      absolutePath(conf.getValue(key), file).map(Directory(_))

    def absolutePath(value: ConfigValue, file: ObtFile): Result[Path] = {
      val rawPath = value.unwrapped().asInstanceOf[String]

      def assumedImmutable(path: Path) = path startsWith NamingConventions.AfsDist.path

      try {
        val path = Paths.get(rawPath)
        def fail(why: String) = file.failure(value, s"Invalid path $path: $why")
        if (!path.isAbsolute) fail("you cannot use a relative path")
        else if (!assumedImmutable(path)) fail("cannot use a non-disted path here")
        else Success(path)
      } catch {
        case NonFatal(t) => file.failure(value, s"Invalid path $rawPath: ${t.getMessage}")
      }
    }

    private def toPartialScopeId(partialScope: String): Option[PartialScopeId] =
      partialScope match {
        case "." => Some(PartialScopeId.RootPartialScopeId)
        case _ =>
          val seq = RelaxedIdString.parts(partialScope)
          if (seq.exists(_.isDefined)) {
            val Seq(meta, bundle, module, tpe) = seq
            Some(PartialScopeId(meta, bundle, module, tpe))
          } else None
      }

    def expandPartialScope(origin: ObtFile, partialScope: String, validScopes: Set[ScopeId]): Result[Set[ScopeId]] = {
      def validField(scopeField: String, filterField: Option[String]): Boolean =
        filterField match {
          case None      => true
          case Some("")  => true
          case Some(str) => str == scopeField
        }

      val partialScopeId = toPartialScopeId(partialScope)
      partialScopeId match {
        case Some(partialScopeId) =>
          val scopes = validScopes.filter(s =>
            validField(s.meta, partialScopeId.meta) &&
              validField(s.bundle, partialScopeId.bundle) &&
              validField(s.module, partialScopeId.module) &&
              validField(s.tpe, partialScopeId.tpe))

          if (scopes.nonEmpty) Success(scopes)
          else
            origin.failure(
              v = conf.root(),
              msg = s"$partialScope is an invalid scope or partial scope"
            )
        case None =>
          origin.failure(
            v = conf.root(),
            msg = s"'$partialScope' is an invalid format for a scope or partial scope"
          )
      }
    }

    def listOfListOrEmpty(key: String, file: ObtFile): Result[Seq[Seq[String]]] =
      Result
        .sequence {
          if (conf.hasPath(key)) {
            conf.getValue(key).asInstanceOf[ConfigList].iterator().asScala.to(Seq).map(v => valueToStrings(v, file))
          } else Nil
        }
  }

  def merge(
      a: Option[Seq[String]],
      b: Option[Seq[String]]
  ): Option[Seq[String]] = (a, b) match {
    case (Some(x), Some(y)) => Some(x ++ y)
    case (Some(x), None)    => Some(x)
    case (None, Some(y))    => Some(y)
    case (None, None)       => None
  }

}
