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
package optimus.config

import optimus.config.spray.json._
import optimus.graph.cache.PerPropertyCache

object CacheProtocols extends DefaultJsonProtocol {
  sealed abstract class CommonJsonFormat[T <: BasicJsonElement] extends RootJsonFormat[T] {
    def write(c: T): JsValue =
      c match {
        case _: CacheConfig =>
          throw new UnsupportedOperationException("please call CacheConfig.toJsonString to get correct output format")
        case _: ConfigOrReference =>
          throw new UnsupportedOperationException(
            "ConfigReference is an intermediate type, would be implicitly convert to CacheConfig or PropertyConfig, so you cannot serialize it")
      }
  }

  implicit object CacheConfigProtocol extends CommonJsonFormat[CacheConfig] {

    def read(value: JsValue): CacheConfig = {
      val fields: Map[String, JsValue] = value.asJsObject.fields
      parseCacheConfig(requiresName = true, fields)
    }

    def parseCacheConfig(requiresName: Boolean, fields: Map[String, JsValue]): CacheConfig = {
      val invalidKeys = fields.keys.toSet -- Constants.LegalCacheConfigField
      if (invalidKeys.nonEmpty)
        throw new OptimusConfigParsingException(s"CacheConfig object shouldn't have: ${invalidKeys.mkString(", ")}")
      CacheConfig(
        name = fields
          .getOrElse(
            "name",
            if (requiresName)
              throw new OptimusConfigParsingException(
                "error happened during deserialize CacheConfig object: name is a required field")
            else JsNull
          )
          .convertTo[Option[String]]
          .getOrElse(PerPropertyCache.defaultName),
        cacheSizeLimit = fields
          .getOrElse(
            "cacheSize",
            throw new OptimusConfigParsingException(
              "error happened during deserialize CacheConfig object: cacheSize is a required field (1000 is a good starting point for size)")
          )
          .convertTo[Int],
        // this is set to false after this method is called for inline caches
        sharable = true
      )
    }
  }

  implicit object ConfigReferenceProtocol extends CommonJsonFormat[ConfigOrReference] {
    def read(value: JsValue): ConfigOrReference = {
      value match {
        case obj: JsObject if obj.fields.contains(OptimusConfigParser.sectionKey) =>
          ConfigurationReference(
            obj.fields(OptimusConfigParser.sectionKey).asInstanceOf[JsString].value,
            obj.fields(OptimusConfigParser.keyKey).asInstanceOf[JsString].value,
            obj.fields(OptimusConfigParser.origin).asInstanceOf[JsString].value
          )
        case obj: JsObject => InlineConfiguration(obj)

        case _ =>
          throw new IllegalArgumentException(
            s"cannot convert a non-JsObject object to ConfigReference - $value, ${value.getClass}")
      }
    }
  }
}
