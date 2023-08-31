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
package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.text.ObjectDefParser2;

public class PermanentRetentionPolicy implements ValueRetentionPolicy {
  static final PermanentRetentionPolicy template = new PermanentRetentionPolicy();

  static {
    ObjectDefParser2.addParser(template);
  }

  @OmitGeneration
  public PermanentRetentionPolicy() {}

  @Override
  public int hashCode() {
    return this.getClass().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) return false;
    return other.getClass().equals(getClass());
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  public static PermanentRetentionPolicy parse(String def) {
    return ObjectDefParser2.parse(PermanentRetentionPolicy.class, def);
  }
}
