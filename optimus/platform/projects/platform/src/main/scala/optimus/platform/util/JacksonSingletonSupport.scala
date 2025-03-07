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
package optimus.platform.util

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.BeanDescription
import com.fasterxml.jackson.databind.DeserializationConfig
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.Module
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping
import com.fasterxml.jackson.databind.SerializationConfig
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator
import com.fasterxml.jackson.databind.jsontype.TypeSerializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier
import optimus.core.utils.RuntimeMirror
import optimus.platform.util.json.DefaultJsonMapper

import java.util.concurrent.ConcurrentHashMap

/**
 * Jackson doesn't support Scala singleton objects (aka modules) correctly. In fact it reinstantiates the module class
 * during deserialization, which actually causes the original singleton value to be overwritten (see:
 * https://stackoverflow.com/questions/55702041/constructor-newinstance-replaces-scala-object).
 *
 * This Jackson Module replaces serialization for Scala objects so that only the classname is written. During
 * deserialization it looks up the original singleton instance using Scala reflection.
 *
 * You can use it with a Jackson mapper like this:
 * {{{
 * val m = new ObjectMapper
 * m.registerModule(DefaultScalaModule)
 * m.registerModule(JacksonSingletonSupport.createModule)
 * m.enableDefaultTypingAsProperty(DefaultTyping.NON_FINAL, JacksonSingletonSupport.tpe)
 * }}}
 * Or just use the prebuilt mapper from createMapper
 */
object JacksonSingletonSupport {
  def tpe: String = "@type"
  def createModule: Module = {
    val m = new SimpleModule()
    m.setSerializerModifier(SerializerModifier)
    m.setDeserializerModifier(DeserializerModifier)
    m
  }

  def createMapper: ObjectMapper = {
    val ptv = BasicPolymorphicTypeValidator.builder.allowIfSubType(classOf[Any]).build
    DefaultJsonMapper.withJavaTimeBuilder
      .addModule(createModule)
      .activateDefaultTyping(ptv, DefaultTyping.NON_FINAL)
      .activateDefaultTypingAsProperty(ptv, DefaultTyping.NON_FINAL, JacksonSingletonSupport.tpe)
      .enable(MapperFeature.USE_BASE_TYPE_AS_DEFAULT_IMPL)
      .build()
  }
  private val mirror = RuntimeMirror.forClass(this.getClass)

  private val moduleCache: ConcurrentHashMap[Class[_], Option[Any]] = new ConcurrentHashMap()
  private def moduleInstanceForClass(cls: Class[_]): Option[Any] = {
    moduleCache.computeIfAbsent(
      cls,
      { _ =>
        val sym = mirror.classSymbol(cls)
        if (sym.isModuleClass) Some(mirror.reflectModule(sym.module.asModule).instance) else None
      })
  }

  private object ModuleSerializer extends JsonSerializer[Any] {
    override def serialize(value: Any, gen: JsonGenerator, serializers: SerializerProvider): Unit = {
      gen.writeStartObject()
      gen.writeStringField(tpe, value.getClass.getName)
      gen.writeEndObject()
    }

    override def serializeWithType(
        value: Any,
        gen: JsonGenerator,
        serializers: SerializerProvider,
        typeSer: TypeSerializer): Unit =
      serialize(value, gen, serializers)
  }

  private object SerializerModifier extends BeanSerializerModifier {
    override def modifySerializer(
        config: SerializationConfig,
        beanDesc: BeanDescription,
        serializer: JsonSerializer[_]): JsonSerializer[_] =
      if (moduleInstanceForClass(beanDesc.getType.getRawClass).isDefined) ModuleSerializer else serializer
  }

  private object DeserializerModifier extends BeanDeserializerModifier {
    override def modifyDeserializer(
        config: DeserializationConfig,
        beanDesc: BeanDescription,
        deserializer: JsonDeserializer[_]): JsonDeserializer[_] =
      moduleInstanceForClass(beanDesc.getType.getRawClass)
        .map(instance =>
          { (_: JsonParser, _: DeserializationContext) =>
            instance
          }: JsonDeserializer[_])
        .getOrElse(deserializer)
  }
}
