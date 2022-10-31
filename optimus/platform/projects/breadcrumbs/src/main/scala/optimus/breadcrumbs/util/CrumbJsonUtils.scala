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
package optimus.breadcrumbs.util

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.EitherModule
import com.fasterxml.jackson.module.scala.IterableModule
import com.fasterxml.jackson.module.scala.IteratorModule
import com.fasterxml.jackson.module.scala.JacksonModule
import com.fasterxml.jackson.module.scala.MapModule
import com.fasterxml.jackson.module.scala.OptionModule
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.SeqModule
import com.fasterxml.jackson.module.scala.SetModule
import com.fasterxml.jackson.module.scala.TupleModule
import com.fasterxml.jackson.module.scala.introspect.ScalaAnnotationIntrospectorModule
import spray.json.JsValue

object CrumbJsonModule
    extends JacksonModule
    with IteratorModule
    with OptionModule
    with SeqModule
    with IterableModule
    with TupleModule
    with MapModule
    with SetModule
    with ScalaAnnotationIntrospectorModule
    with EitherModule {
  override def getModuleName = "CrumbJsonModule"
}

object CrumbJsonUtils {
  def defaultObjectMapper(): ObjectMapper = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)

    val ToStringModule = new SimpleModule()
    ToStringModule.addSerializer(classOf[AnyRef], ToStringSerializer.instance)
    objectMapper.registerModule(ToStringModule)
    objectMapper.registerModule(CrumbJsonModule)

    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    objectMapper.configure(SerializationFeature.FAIL_ON_SELF_REFERENCES, false)
    objectMapper.configure(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS, false)
    objectMapper.setSerializationInclusion(Include.ALWAYS)

    objectMapper
  }

  def objectToJsValue[T](obj: T): JsValue = {
    import spray.json.enrichString
    defaultObjectMapper()
      .writeValueAsString(obj)
      .parseJson
  }

  def objectToRichJsValue[T](obj: T): JsValue = {
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)

    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    objectMapper.configure(SerializationFeature.FAIL_ON_SELF_REFERENCES, false)
    objectMapper.configure(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS, false)
    objectMapper.setSerializationInclusion(Include.ALWAYS)

    import spray.json.enrichString
    objectMapper.writeValueAsString(obj).parseJson
  }
}
