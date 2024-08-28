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
package optimus.platform.util.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;

import java.io.IOException;

final class ScalaParserCombinatorsCompatiblePrettyPrinter extends DefaultPrettyPrinter {
  public ScalaParserCombinatorsCompatiblePrettyPrinter() {}

  public ScalaParserCombinatorsCompatiblePrettyPrinter(
      ScalaParserCombinatorsCompatiblePrettyPrinter base, SerializableString rootSeparator) {
    super(base, rootSeparator);
  }

  public ScalaParserCombinatorsCompatiblePrettyPrinter(
      ScalaParserCombinatorsCompatiblePrettyPrinter base) {
    super(base);
  }

  @Override
  public DefaultPrettyPrinter createInstance() {
    return new ScalaParserCombinatorsCompatiblePrettyPrinter(this);
  }

  @Override
  public void beforeObjectEntries(JsonGenerator g) throws IOException {}

  @Override
  public void writeObjectEntrySeparator(JsonGenerator g) throws IOException {
    g.writeRaw(_separators.getObjectEntrySeparator());
    g.writeRaw(' ');
  }
}
