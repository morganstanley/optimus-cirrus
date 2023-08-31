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

import com.ms.silverking.cloud.dht.RetrievalOptions;

/** Impose an ordering so that compatible retrievals may be grouped together. */
class RetrievalOptionsComparator implements Comparator<RetrievalOptions> {
  public static RetrievalOptionsComparator instance = new RetrievalOptionsComparator();

  @Override
  public int compare(RetrievalOptions o1, RetrievalOptions o2) {
    int c;

    c = o1.getRetrievalType().compareTo(o2.getRetrievalType());
    if (c != 0) {
      return c;
    } else {
      c = o1.getWaitMode().compareTo(o2.getWaitMode());
      if (c != 0) {
        return c;
      } else {
        c = o1.getNonExistenceResponse().compareTo(o2.getNonExistenceResponse());
        if (c != 0) {
          return c;
        } else {
          c =
              VersionConstraintComparator.instance.compare(
                  o1.getVersionConstraint(), o2.getVersionConstraint());
          if (c != 0) {
            return c;
          } else {
            c = Boolean.compare(o1.getVerifyChecksums(), o2.getVerifyChecksums());
            if (c != 0) {
              return c;
            } else {
              return 0;
            }
          }
        }
      }
    }
  }
}
