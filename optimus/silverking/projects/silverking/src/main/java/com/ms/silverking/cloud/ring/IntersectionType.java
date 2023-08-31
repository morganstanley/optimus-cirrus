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
package com.ms.silverking.cloud.ring;

/**
 * disjoint: aaaaaa bbbbbb isomorphic: aaaaaa bbbbbb abPartial: aaaaaa bbbbbb baPartial: aaaaaa
 * bbbbbb aSubsumesB: aaaaaa aaaaaaaa aaaaaa bbbb bbbb bbbb bSubsumesA: aaaa aaaa aaaa bbbbbb
 * bbbbbbbb bbbbbb wrappedPartial: aaaa aaaa aaaaaa bbbbbb bbbb bbbb (due to wrapping, a contains
 * both the start and end of b and vice versa)
 *
 * <p>nonIdenticalAllRingspace: a&b both contain the entire ringspace, but have distinct starts and
 * ends We currently prevent this by normalizing all entire ringspace RingRegions This type is
 * included for completeness
 */
public enum IntersectionType {
  disjoint,
  isomorphic,
  abPartial,
  baPartial,
  aSubsumesB,
  bSubsumesA,
  wrappedPartial,
  nonIdenticalAllRingspace
}
