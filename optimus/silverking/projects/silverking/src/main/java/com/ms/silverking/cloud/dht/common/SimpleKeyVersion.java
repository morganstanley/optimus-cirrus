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
package com.ms.silverking.cloud.dht.common;

public class SimpleKeyVersion implements KeyVersion {
  private final DHTKey key;
  private final Version version;

  public SimpleKeyVersion(DHTKey key, Version version) {
    this.key = key;
    this.version = version;
  }

  public SimpleKeyVersion(DHTKey key, long version) {
    this(key, new SimpleVersion(version));
  }

  @Override
  public DHTKey getKey() {
    return key;
  }

  @Override
  public Version getVersion() {
    return version;
  }

  @Override
  public int hashCode() {
    return (int) (key.getLSL() | version.versionAsLong());
  }

  @Override
  public boolean equals(Object other) {
    KeyVersion oKeyVersion;

    oKeyVersion = (KeyVersion) other;
    return key.equals(oKeyVersion.getKey()) && version.equals(oKeyVersion.getVersion());
  }

  @Override
  public String toString() {
    return key + ":" + version;
  }
}
