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
package com.ms.silverking.code;

/**
 * Simplifies constraint checking. Similar to assertion except that constraints are not
 * disabled. Also similar to Guava Precondition class except that this is intended
 * for general usage, not just method preconditions.
 */
public class Constraint {
  public static void ensureNotNull(Object o) {
    if (o == null) {
      throw new ConstraintViolationException("ensureNotNull failed");
    }
  }

  public static void ensureNull(Object o) {
    if (o != null) {
      throw new ConstraintViolationException("ensureNull failed: " + o);
    }
  }

  public static void ensureNotEqual(int i1, int i2) {
    if (i1 != i2) {
      throw new ConstraintViolationException("ensureNotEqual failed: " + i1 + " " + i2);
    }
  }

  public static void ensureNonZero(int i) {
    if (i == 0) {
      throw new ConstraintViolationException("ensureNonZero failed: " + i);
    }
  }

  /**
   * Check bounds <b>inclusive<b/>
   *
   * @param min min bound
   * @param max max bound
   * @param i   the integer value to check inclusively in min/max bound
   */
  public static void checkBounds(int min, int max, int i) {
    checkBounds(min, max, i, "");
  }

  /**
   * Check bounds <b>inclusive<b/>
   *
   * @param min min bound
   * @param max max bound
   * @param i   the integer value to check inclusively in min/max bound
   * @param msg error msg
   */
  public static void checkBounds(int min, int max, int i, String msg) {
    if (max < min) {
      String possibleFailure = max < 0 ?
          "the integer value for max bound is possibly overflow" :
          "the min/max bound might be specified in wrong order (or with wrong value)";
      throw new ConstraintViolationException(
          "max bound [" + max + "] is smaller than min bound [" + min + "], " + possibleFailure);
    }

    if (i < min) {
      throw new ConstraintViolationException("checkBounds min bound failed: " + i + " < " + min + " " + msg);
    } else if (i > max) {
      throw new ConstraintViolationException("checkBounds max bound failed: " + i + " > " + max + " " + msg);
    }
  }
}
