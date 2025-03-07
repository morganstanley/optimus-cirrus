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

import static optimus.graph.DiagnosticSettings.enablePerNodeTPDMask;
import static optimus.graph.DiagnosticSettings.tweakUsageQWords;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;

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
    if (maskLength != maskQWords)
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
    for (int index : indexes) {
      int offset = (index - 1) >>> 6; // [REVERSE_INDEX_COMPUTE]
      int shift = (index - 1) & 63;
      // use % not &, as (offset & maskQWord-1) only valid when maskQWords is a power of 2
      r[offset % maskQWords] |= 1L << shift;
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
    return CoreUtils.toIntArray(toIndexList());
  }

  public ArrayList<Integer> toIndexList() {
    return CoreUtils.maskToIndexList(toArray());
  }

  public String stringEncoded() {
    return stringEncoded(toArray());
  }

  public String stringEncodedFixedWidth() {
    return stringEncodedFixedWidth(toArray());
  }

  public int countOfSetBits() {
    return (int) Arrays.stream(toArray()).map(Long::bitCount).sum();
  }

  // encodes 33,34 ... 64 as ffffffff00000000 not -100000000
  public static String stringEncoded(long[] mask) {
    StringBuilder r = new StringBuilder();
    for (int i = mask.length - 1; i >= 0; i--) {
      if (!r.isEmpty()) r.append('_');
      if (mask[i] != 0) {
        // avoid negative sign when top bit is set
        r.append(StringUtils.stripStart(Long.toHexString(mask[i]), "0"));
      } else if (!r.isEmpty()) r.append('0');
    }
    return r.isEmpty() ? "0" : r.toString();
  }

  public static TPDMask stringDecoded(String str) {
    if (str == null) return new TPDMask();
    String[] parts = str.split("_");
    long[] mask = new long[parts.length];
    for (int i = 0; i < parts.length; i++)
      if (!parts[i].isEmpty()) mask[mask.length - 1 - i] = Long.parseUnsignedLong(parts[i], 16);
    return TPDMask.fromArray(mask);
  }

  // fixed width representation - makes cases such as below easier to read
  // parent: 00000003FFFE0020_0000000000140000
  // child1: 00000003FFFE0020_0000000000000000
  // child2: 00000000000E0020_0000000000140000
  public static String stringEncodedFixedWidth(long[] mask) {
    StringBuilder r = new StringBuilder();
    for (int i = mask.length - 1; i >= 0; i--) {
      if (!r.isEmpty()) r.append('_');
      r.append(Strings.padStart(Long.toHexString(mask[i]), 16, '0'));
    }
    return r.toString().toUpperCase();
  }

  /** Returns true if all bits are set otherwise false */
  public boolean allBitsAreSet() {
    // was the following
    // return equals(poison);
    // but comment on equals says "Equals should be used for testing only"
    // this method is used in HotspotsTable and XSFTRemapper
    long[] mask = toArray();
    // all bits are set if all values in mask are -1
    // the poison mask is currently all bits set
    // overflow / many tweaks can result in all bits being set
    // seems dangerous to potentially conflate the two
    // ideally would stop last bit ever being set unless poison
    for (long l : mask) if (l != -1L) return false;
    return true;
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
