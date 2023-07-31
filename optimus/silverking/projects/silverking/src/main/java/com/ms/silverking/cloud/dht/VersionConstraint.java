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
package com.ms.silverking.cloud.dht;

import com.ms.silverking.text.ObjectDefParser2;

/**
 * Constraints on a retrieval operation. These constraints define which value should
 * be retrieved in the context of a multi-versioned namespace.
 */
public class VersionConstraint {
  private final long min;
  private final long max;
  private final Mode mode;
  private final long maxCreationTime;

  /**
   * When multiple versions match this constraint, the Mode determines which version to return
   */
  public enum Mode {LEAST, GREATEST}

  /**
   * Greatest of all existent versions
   */
  public static final VersionConstraint greatest = new VersionConstraint();
  /**
   * Least of all existent versions
   */
  public static final VersionConstraint least = new VersionConstraint(Long.MIN_VALUE, Long.MAX_VALUE, Mode.LEAST);
  /**
   * By default, return the greatest of all existent versions
   */
  public static final VersionConstraint defaultConstraint = greatest;
  /**
   * No limit on time of creation
   */
  public static final long noCreationTimeLimit = Long.MAX_VALUE;

  private static final int maxSpecialValues = 10;

  static {
    ObjectDefParser2.addParser(defaultConstraint);
  }

  private VersionConstraint() {
    this(Long.MIN_VALUE, Long.MAX_VALUE, Mode.GREATEST);
  }

  /**
   * Create a VersionConstraint that matches only a single specified version
   *
   * @param version the version to match
   * @return a VersionConstraint that matches only a single specified version
   */
  public static VersionConstraint exactMatch(long version) {
    return new VersionConstraint(version, version, Mode.GREATEST);
  }

  /**
   * Construct a fully-specified VersionConstraint
   *
   * @param min             minimum version to match inclusive
   * @param max             maximum version to match inclusive
   * @param mode            if multiple versions match, mode specifies which to return
   * @param maxCreationTime the maximum storage time that may match inclusive
   */
  public VersionConstraint(long min, long max, Mode mode, long maxCreationTime) {
    this.min = min;
    if (min != max &&
        this.min != Long.MIN_VALUE &&
        this.min > Long.MIN_VALUE + maxSpecialValues &&
        mode != Mode.LEAST) {
      throw new RuntimeException("nonmin not yet supported: " + this.min);
    }
    this.max = max;
    this.mode = mode;
    this.maxCreationTime = maxCreationTime;

    // TODO (OPTIMUS-0000): oldest used internally, think about whether the support
    // is sufficient for user usage. if not, we need to enforce restriction in client
    // api and not here any more
    //if (mode == Mode.OLDEST) {
    //    throw new RuntimeException("OLDEST not yet supported");
    //}
  }

  /**
   * Construct a VersionConstraint with no creation time restriction
   *
   * @param min  minimum version to match inclusive
   * @param max  maximum version to match inclusive
   * @param mode if multiple versions match, mode specifies which to return
   */
  public VersionConstraint(long min, long max, Mode mode) {
    this(min, max, mode, noCreationTimeLimit);
  }

  /**
   * Create a VersionConstraint that matches the least version above or equal to a given version threshold
   * and no creation time restriction.
   *
   * @param threshold the version threshold
   * @return the new VersionConstraint
   */
  public static VersionConstraint minAboveOrEqual(long threshold) {
    return new VersionConstraint(threshold, Long.MAX_VALUE, Mode.LEAST);
  }

  /**
   * Create a VersionConstraint that matches the greatest version above or equal to a given version threshold
   * and no creation time restriction.
   *
   * @param threshold the version threshold
   * @return the new VersionConstraint
   */
  public static VersionConstraint maxAboveOrEqual(long threshold) {
    return new VersionConstraint(threshold, Long.MAX_VALUE, Mode.GREATEST);
  }

  /**
   * Create a VersionConstraint that matches the greatest version below or equal to a given version threshold
   * and no creation time restriction.
   *
   * @param threshold the version threshold
   * @return the new VersionConstraint
   */
  public static VersionConstraint maxBelowOrEqual(long threshold) {
    return new VersionConstraint(Long.MIN_VALUE, threshold, Mode.GREATEST);
  }

  /**
   * true if this VersionConstraint's version bounds match the given version, false otherwise.
   *
   * @param version version to test
   * @return True if this VersionConstraint's version bounds match the given version, false otherwise.
   */
  public boolean matches(long version) {
    return version >= min && version <= max;
  }

  /**
   * The minimum version bound
   *
   * @return the minimum version bound
   */
  public long getMin() {
    return min;
  }

  /**
   * The maximum version bound
   *
   * @return the maximum version bound
   */
  public long getMax() {
    return max;
  }

  /**
   * The maximum creation time bound
   *
   * @return the maximum creation time bound
   */
  public long getMaxCreationTime() {
    return maxCreationTime;
  }

  /**
   * The Mode used to select a value when multiple versions match
   *
   * @return the Mode used to select a value when multiple versions match
   */
  public Mode getMode() {
    return mode;
  }

  /**
   * Create a copy of this VersionConstraint with a new minimum version bound
   *
   * @param min the new minimum version bound
   * @return a copy of this VersionConstraint with a new minimum version bound
   */
  public VersionConstraint min(long min) {
    return new VersionConstraint(min, max, mode, maxCreationTime);
  }

  /**
   * Create a copy of this VersionConstraint with a new maximum version bound
   *
   * @param max the new maximum version bound
   * @return a copy of this VersionConstraint with a new maximum version bound
   */
  public VersionConstraint max(long max) {
    return new VersionConstraint(min, max, mode, maxCreationTime);
  }

  /**
   * Create a copy of this VersionConstraint with a new version selection Mode
   *
   * @param mode the new version selection Mode
   * @return a copy of this VersionConstraint with a new version selection Mode
   */
  public VersionConstraint mode(Mode mode) {
    return new VersionConstraint(min, max, mode, maxCreationTime);
  }

  /**
   * Create a copy of this VersionConstraint with a new maximum creation time
   *
   * @param maxCreationTime the new maximum creation time
   * @return a copy of this VersionConstraint with a new maximum creation time
   */
  public VersionConstraint maxCreationTime(long maxCreationTime) {
    return new VersionConstraint(min, max, mode, maxCreationTime);
  }

  @Override
  public int hashCode() {
    return (int) (min * max * maxCreationTime) + mode.ordinal();
  }

  @Override
  public boolean equals(Object other) {
    VersionConstraint oVC;

    oVC = (VersionConstraint) other;
    return this.min == oVC.min &&
           this.max == oVC.max &&
           this.mode == oVC.mode &&
           this.maxCreationTime == oVC.maxCreationTime;
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  /**
   * true if the version bounds of this VersionConstraint overlap the bounds of the given constraint; false otherwise
   *
   * @param other the VersionConstraint to test against this constraint
   * @return true if the version bounds of this VersionConstraint overlap the bounds of the given constraint;
   * false otherwise
   */
  public boolean overlaps(VersionConstraint other) {
    if (other.max < this.min) {
      return false;
    } else if (other.min > this.max) {
      return false;
    } else {
      return true;
    }
  }
}
