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
package com.ms.silverking.cloud.dht.meta;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.ms.silverking.cloud.meta.VersionedDefinition;
import com.ms.silverking.io.FileUtil;
import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

public class IpAliasConfiguration implements VersionedDefinition {

  private final long version;
  private final Map<String, String> ipAliasMap;

  public static final IpAliasConfiguration emptyTemplate = new IpAliasConfiguration(0, null);

  public IpAliasConfiguration(long version, Map<String, String> ipAliasMap) {
    this.version = version;
    this.ipAliasMap = ipAliasMap;
  }

  static {
    ObjectDefParser2.addParser(emptyTemplate, FieldsRequirement.ALLOW_INCOMPLETE);
  }

  public static IpAliasConfiguration parse(String def, long version) {
    IpAliasConfiguration instance;
    instance = ObjectDefParser2.parse(IpAliasConfiguration.class, def);
    return instance.version(version);
  }

  public static IpAliasConfiguration readFromFile(String fileName) throws IOException {
    return readFromFile(new File(fileName));
  }

  public static IpAliasConfiguration readFromFile(File f) throws IOException {
    String def;

    def = FileUtil.readFileAsString(f);
    return parse(def, 0);
  }

  public IpAliasConfiguration version(long version) {
    return new IpAliasConfiguration(version, this.ipAliasMap);
  }

  public IpAliasConfiguration ipAliasMap(Map<String, String> ipAliasMap) {
    return new IpAliasConfiguration(this.version, ipAliasMap);
  }

  @Override
  public long getVersion() {
    return version;
  }

  public Map<String, String> getIPAliasMap() {
    return ipAliasMap;
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }
}
