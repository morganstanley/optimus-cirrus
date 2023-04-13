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
package com.ms.silverking.numeric;

public class EntropyCalculator {
  public static double computeEntropy(byte[] b) {
    return computeEntropy(b, 0, b.length);
  }

  public static double computeEntropy(byte[] b, int offset, int length) {
    long[] occurrences;
    double sum;

    occurrences = new long[NumConversion.BYTE_MAX_UNSIGNED_VALUE + 1];
    for (int i = 0; i < length; i++) {
      occurrences[NumConversion.byteToPositiveInt(b[offset + i])]++;
    }
    sum = 0.0;
    for (int i = 0; i < occurrences.length; i++) {
      if (occurrences[i] > 0) {
        double p;
        double lp;

        p = (double) occurrences[i] / (double) length;
        lp = NumUtil.log(2.0, p);
        sum += p * lp;
      }
    }
    return -sum;
  }
}
