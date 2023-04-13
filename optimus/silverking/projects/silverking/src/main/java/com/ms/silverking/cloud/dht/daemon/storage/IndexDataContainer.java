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
package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.Set;

import com.ms.silverking.cloud.dht.VersionConstraint;

/**
 * A container for index data. This is implemented concretely by two subtypes, {@link RAMOffsetList} and
 * {@link NamespaceStoreIndex.SegmentId}. These correspond (respectively) to index data where there is more than one
 * version to index and where this is only a single version to index.
 */
public interface IndexDataContainer extends Iterable<Integer> {
  int EmptyContainer = -1;

  /**
   * Returns the segment number from the index data. Although attempts may be made to satisfy the given {@link
   * VersionConstraint}, this is implementation-dependent and callers should not rely on this.
   * <p>
   * If supplied, the given {@link ValidityVerifier} can be used to verify the storage state of the data. Otherwise,
   * the caller should supply <tt>null</tt>.
   * <p>
   * The implementation wil return either a segment number or {@link OffsetList#NO_MATCH_FOUND}.
   *
   * @param vc               a constraint on the version of the key to query index data for
   * @param validityVerifier optional verification to be run on the data
   * @return a segment number
   */
  int getSegmentNumber(VersionConstraint vc, ValidityVerifier validityVerifier);

  /**
   * Remove entries from the container which correspond to the given segment numbers.
   *
   * @param segmentsToRemove The segments which should be removed from the container
   */
  void removeEntriesBySegment(Set<Integer> segmentsToRemove);

  /**
   * Update entries in the container based on modifications performed during compaction which may have impacted the
   * index data in this container.
   *
   * @param entriesToUpdate The compaction modifications which need to be reflected by the update operation
   */
  void updateEntriesByMatch(Set<CompactorModifiedEntry> entriesToUpdate);

  /**
   * Returns <tt>true</tt> if the container is empty, <tt>false</tt> otherwise.
   *
   * @return whether the container is empty
   */
  boolean isEmpty();

  /**
   * Returns the size of the container, i.e. the number of versions that this index data contains.
   *
   * @return the size of the container
   */
  int size();
}
