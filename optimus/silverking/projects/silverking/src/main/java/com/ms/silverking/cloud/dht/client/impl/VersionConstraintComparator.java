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
package com.ms.silverking.cloud.dht.client.impl;

import java.util.Comparator;

import com.ms.silverking.cloud.dht.VersionConstraint;

/**
 * Impose an ordering so that compatible retrievals may be grouped together.
 */
class VersionConstraintComparator implements Comparator<VersionConstraint> {
  public static final VersionConstraintComparator instance = new VersionConstraintComparator();

  @Override
  public int compare(VersionConstraint c1, VersionConstraint c2) {
    int c;

    c = c1.getMode().compareTo(c2.getMode());
    if (c != 0) {
      return c;
    } else {
      if (c1.getMin() < c2.getMin()) {
        return -1;
      } else if (c1.getMin() > c2.getMin()) {
        return 1;
      }
      if (c1.getMax() < c2.getMax()) {
        return -1;
      } else if (c1.getMax() > c2.getMax()) {
        return 1;
      }
      if (c1.getMaxCreationTime() < c2.getMaxCreationTime()) {
        return -1;
      } else if (c1.getMaxCreationTime() > c2.getMaxCreationTime()) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}
