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
package com.ms.silverking.collection.cuckoo;

/** Pairs the key with the value that the key maps to in the hash table. */
class IntKeyIntEntry {
  private final int key;
  private final int value;

  IntKeyIntEntry(int key, int value) {
    this.key = key;
    this.value = value;
  }

  public int getKey() {
    return key;
  }

  public int getValue() {
    return value;
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(key);
  }

  @Override
  public boolean equals(Object o) {
    IntKeyIntEntry other;

    other = (IntKeyIntEntry) o;
    return key == other.key && value == other.value;
  }

  @Override
  public String toString() {
    return String.format("%d:%d", key, value);
  }
}
