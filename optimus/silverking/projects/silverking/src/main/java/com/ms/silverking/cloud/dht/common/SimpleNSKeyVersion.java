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

public class SimpleNSKeyVersion extends SimpleNSKey {
  private final long version;

  public SimpleNSKeyVersion(Namespace ns, DHTKey key, Version version) {
    super(ns, key);
    this.version = version.versionAsLong();
  }

  public SimpleNSKeyVersion(NSKey nsKey, Version version) {
    this(nsKey.getNamespace(), nsKey.getKey(), version);
  }

  @Override
  public int hashCode() {
    return super.hashCode() ^ (int) version;
  }

  @Override
  public boolean equals(Object o) {
    SimpleNSKeyVersion other;

    other = (SimpleNSKeyVersion) o;
    return this.version == other.version && super.equals(other);
  }
}
