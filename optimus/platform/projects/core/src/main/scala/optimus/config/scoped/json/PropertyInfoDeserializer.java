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
package optimus.config.scoped.json;

import java.io.IOException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import optimus.entity.EntityInfoRegistry;
import optimus.graph.PropertyInfo;

class PropertyInfoDeserializer extends StdDeserializer<PropertyInfo<?>> {

  protected PropertyInfoDeserializer() {
    this(null);
  }

  protected PropertyInfoDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public PropertyInfo<?> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    JsonNode jsonNode = p.getCodec().readTree(p);
    String propertyInfo = jsonNode.textValue();
    int dotIdx = propertyInfo.lastIndexOf('.');
    int afterDotIdx = dotIdx + 1;
    if (dotIdx < 0 || afterDotIdx == propertyInfo.length()) {
      throw new JsonParseException(p, "Could not parse property info: " + propertyInfo);
    }
    String runtimeClassName = propertyInfo.substring(0, dotIdx);
    try {
      Class<?> runtimeClass = Class.forName(runtimeClassName);
      String name = propertyInfo.substring(afterDotIdx);
      boolean isModule = runtimeClassName.endsWith("$");
      return isModule
          ? EntityInfoRegistry.getModuleInfo(runtimeClass).propertyMetadata().apply(name)
          : EntityInfoRegistry.getInfo(runtimeClass).propertyMetadata().apply(name);
    } catch (ClassNotFoundException ex) {
      throw new JsonParseException(p, "Could not find class for " + runtimeClassName);
    }
  }
}
