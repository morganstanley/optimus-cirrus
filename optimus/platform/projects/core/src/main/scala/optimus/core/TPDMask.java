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
package optimus.core;

import static optimus.core.CoreUtils.maskToIndexList;
import static optimus.core.CoreUtils.toIntArray;
import static optimus.graph.DiagnosticSettings.enablePerNodeTPDMask;
import static optimus.graph.DiagnosticSettings.tweakUsageQWords;

import java.io.Serializable;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* aka TweakPropertyDependenciesMask injected by entity agent (in TPDMaskAdapter) [SEE_MASK_SUPPORT_GENERATION] */
public final class TPDMask implements Serializable, Cloneable {
  private static final int maskQWords = enablePerNodeTPDMask ? tweakUsageQWords : 2;
  public static final TPDMask empty = new TPDMask();
  public static final TPDMask poison = poisonMask();
  public long m0, m1;

  private static final Logger log = LoggerFactory.getLogger(TPDMask.class);

  private static TPDMask poisonMask() {
    long[] template = new long[maskQWords];
    // ie, we depend on all tweakables (for APIs that try to capture current state)
    Arrays.fill(template, -1);
    return fromArray(template);
  }

  /** Creates a new TPDMask from an array (which could be shorter than QWords) */
  public static TPDMask fromArray(long[] maskArray) {
    int maskLength = maskArray.length;
    if (maskLength > maskQWords)
      log.debug("Compressing mask from length " + maskLength + " to length " + maskQWords);
    TPDMask mask = new TPDMask();
    if (maskArray.length != maskQWords)
      // this'll chop off the end if maskArray is longer than qWords
      maskArray = Arrays.copyOf(maskArray, maskQWords);
    fillFromArray(mask, maskArray);
    return mask;
  }

  /**
   * Fills the mask with already properly sized array Base version and could be re-written by
   * entityagent [SEE_MASK_SUPPORT_GENERATION]
   */
  private static void fillFromArray(TPDMask mask, long[] maskArray) {
    mask.m0 = maskArray[0];
    mask.m1 = maskArray[1];
  }

  /** Base version and could be re-written by entityagent [SEE_MASK_SUPPORT_GENERATION] */
  public long[] toArray() {
    return new long[] {m0, m1};
  }

  /**
   * Base version and could be re-written by entityagent [SEE_MASK_SUPPORT_GENERATION] Consider
   * adding the following: if (Settings.schedulerAsserts && this == empty) throw new
   * IllegalArgumentException();
   */
  public void merge(TPDMask mask) {
    m0 |= mask.m0;
    m1 |= mask.m1;
  }

  /** Base version and could be re-written by entityagent [SEE_MASK_SUPPORT_GENERATION] */
  public boolean intersects(TPDMask mask) {
    return (m0 & mask.m0) != 0 || (m1 & mask.m1) != 0;
  }

  /** Base version and could be re-written by entityagent [SEE_MASK_SUPPORT_GENERATION] */
  public boolean subsetOf(TPDMask mask) {
    return (m0 | mask.m0) == mask.m0 && (m1 | mask.m1) == mask.m1;
  }

  public static long[] indexListToMask(int[] indexes) {
    long[] r = new long[maskQWords];
    int tweakUsageWrapAround = maskQWords - 1;

    for (int index : indexes) {
      int offset = (index - 1) >>> 6; // [REVERSE_INDEX_COMPUTE]
      int shift = (index - 1) & 63;
      r[offset & tweakUsageWrapAround] |= 1L << shift;
    }
    return r;
  }

  /* should not be called with index = 0 */
  public static TPDMask fromIndex(int index) {
    return fromArray(indexListToMask(new int[] {index}));
  }

  public static TPDMask fromIndexes(int[] indexes) {
    return fromArray(indexListToMask(indexes));
  }

  public int[] toIndexes() {
    return toIntArray(maskToIndexList(toArray()));
  }

  public String stringEncoded() {
    return stringEncoded(toArray());
  }

  public static String stringEncoded(long[] mask) {
    StringBuilder r = new StringBuilder();
    for (int i = mask.length - 1; i >= 0; i--) {
      if (r.length() > 0) r.append('_');
      if (mask[i] != 0) r.append(Long.toString(mask[i], 16));
      else if (r.length() > 0) r.append('0');
    }
    return r.length() == 0 ? "0" : r.toString();
  }

  public static TPDMask stringDecoded(String str) {
    if (str == null) return new TPDMask();
    String[] parts = str.split("_");
    long[] mask = new long[parts.length];

    for (int i = 0; i < parts.length; i++)
      if (!parts[i].isEmpty()) mask[mask.length - 1 - i] = Long.valueOf(parts[i], 16);
    return TPDMask.fromArray(mask);
  }

  /** Returns true if all bits are set otherwise false */
  public final boolean allBitsAreSet() {
    return equals(poison);
  }

  /** Equals should be used for testing only */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TPDMask) return Arrays.equals(toArray(), ((TPDMask) obj).toArray());
    return false;
  }

  @Override
  public String toString() {
    return stringEncoded();
  }

  public boolean empty() {
    return this.equals(empty);
  }

  public boolean nonEmpty() {
    return !this.equals(empty);
  }

  public TPDMask dup() {
    var out = new TPDMask();
    out.merge(this);
    return out;
  }
}
