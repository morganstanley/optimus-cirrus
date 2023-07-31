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
package optimus.stratosphere.bootstrap;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class IniFile {
  public static Map<String, Properties> parse(Reader reader) throws IOException {
    HashMap<String, Properties> result = new HashMap<>();
    new Properties() {
      private Properties section;

      @Override
      public Object put(Object key, Object value) {
        String header = (key + " " + value).trim();
        return (header.startsWith("[") && header.endsWith("]"))
            ? result.put(header.substring(1, header.length() - 1), section = new Properties())
            : section.put(key, value);
      }
    }.load(reader);
    return result;
  }

  public void save(Writer writer, Map<String, Properties> properties) throws IOException {
    for (Map.Entry<String, Properties> section : properties.entrySet()) {
      writer.write(section.getKey() + System.lineSeparator());
      writer.write(System.lineSeparator());

      for (Map.Entry<Object, Object> entry : section.getValue().entrySet()) {
        writer.write(entry.getKey().toString());
        writer.write(" = ");
        writer.write(entry.getValue().toString());
        writer.write(System.lineSeparator());
      }
    }
  }
}
