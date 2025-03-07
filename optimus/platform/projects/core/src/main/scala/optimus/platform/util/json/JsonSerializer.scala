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
/* The ClassTagExtensions in this file were copied from Jackson 2.13.x for forward source compatibility for easier migration.
 * https://github.com/FasterXML/jackson-module-scala/blob/2.19/src/main/scala/com/fasterxml/jackson/module/scala/ClassTagExtensions.scala
 * For that particular code:
 *
 * Copyright FasterXML/jackson-module-scala authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package optimus.platform.util.json

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.ScalaObjectMapper

import java.io.StringWriter

// Copied from Jackson 2.13.x for forwards source compat for easier migration
object ClassTagExtensions {
  def ::(o: JsonMapper): JsonMapper with ScalaObjectMapper = new Mixin(o)
  def ::(o: ObjectMapper): ObjectMapper with ScalaObjectMapper = new ObjectMapperMixin(o)

  final class Mixin private[ClassTagExtensions] (mapper: JsonMapper) extends JsonMapper(mapper) with ScalaObjectMapper

  final class ObjectMapperMixin private[ClassTagExtensions] (mapper: ObjectMapper)
      extends ObjectMapper(mapper)
      with ScalaObjectMapper
}

object DefaultJsonMapper {

  def legacyBuilder: JsonMapper.Builder = JsonMapper
    .builder()
    .addModule(DefaultScalaModule)

  // !!! This one needs to be `def` as clients modify it !!!
  def legacy: JsonMapper with ScalaObjectMapper =
    legacyBuilder.build() :: ClassTagExtensions

  def withJavaTimeBuilder: JsonMapper.Builder = legacyBuilder.addModule(new JavaTimeModule)

  def withJavaTime: JsonMapper with ScalaObjectMapper =
    withJavaTimeBuilder.build() :: ClassTagExtensions
}

object JsonSerializer {

  def toJson(m: Map[String, Any]): String = {
    val prettyPrinter = new ScalaParserCombinatorsCompatiblePrettyPrinter
    prettyPrinter.indentObjectsWith(DefaultPrettyPrinter.NopIndenter.instance)
    prettyPrinter.indentArraysWith(DefaultPrettyPrinter.FixedSpaceIndenter.instance)

    val writer = new StringWriter()
    DefaultJsonMapper.legacy.writer(prettyPrinter).writeValue(writer, m)
    writer.toString
  }
}
