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
package optimus.breadcrumbs.kafka;

import java.util.Map;

import optimus.crypto.Encryptor;
import optimus.crypto.EncryptorDecryptor;
import org.apache.kafka.common.serialization.Serializer;

public class EncryptionStringSerializer implements Serializer<String> {

  private Encryptor encryptor;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    String versionPropertyName = "encryption.serializer.version";
    int version = getIntProperty(configs, versionPropertyName, 1);

    encryptor = EncryptorDecryptor.buildEncryptor(version);
  }

  @Override
  public byte[] serialize(String topic, String data) {
    return encryptor.encrypt(data);
  }

  @Override
  public void close() {

  }

  private boolean getBooleanProperty(Map<String, ?> configs, String propertyName, boolean defaultValue) {
    Object compressValue = configs.get(propertyName);
    boolean value;
    if (compressValue == null) {
      value = defaultValue;
    } else {
      value = Boolean.parseBoolean((String) compressValue);
    }

    return value;
  }

  private int getIntProperty(Map<String, ?> configs, String propertyName, int defaultValue) {
    Object intValue = configs.get(propertyName);
    int value;
    if (intValue == null) {
      value = defaultValue;
    } else {
      value = Integer.parseInt((String) intValue);
    }

    return value;
  }

}
