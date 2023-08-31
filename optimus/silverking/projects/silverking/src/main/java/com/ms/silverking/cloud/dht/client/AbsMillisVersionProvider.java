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
package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.time.AbsMillisTimeSource;

/** Provides versions from an AbsMillisTimeSource. */
public class AbsMillisVersionProvider implements VersionProvider {
  private final AbsMillisTimeSource absMillisTimeSource;

  public AbsMillisVersionProvider(AbsMillisTimeSource absMillisTimeSource) {
    this.absMillisTimeSource = absMillisTimeSource;
  }

  @Override
  public long getVersion() {
    return absMillisTimeSource.absTimeMillis();
  }

  @Override
  public int hashCode() {
    return absMillisTimeSource.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (this.getClass() != o.getClass()) {
      return false;
    }

    AbsMillisVersionProvider other = (AbsMillisVersionProvider) o;
    return absMillisTimeSource.equals(other.absMillisTimeSource);
  }
}
