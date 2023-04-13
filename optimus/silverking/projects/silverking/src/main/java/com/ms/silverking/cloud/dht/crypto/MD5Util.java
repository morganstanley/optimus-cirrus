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
package com.ms.silverking.cloud.dht.crypto;

import com.ms.silverking.numeric.NumConversion;

public class MD5Util {
  public static int hashCode(byte[] md5) {
    return hashCode(md5, 0);
  }

  public static int hashCode(byte[] md5, int offset) {
    return NumConversion.bytesToInt(md5, offset) ^ NumConversion.bytesToInt(md5,
        offset + NumConversion.BYTES_PER_INT) ^ NumConversion.bytesToInt(md5,
        offset + 2 * NumConversion.BYTES_PER_INT) ^ NumConversion.bytesToInt(md5,
        offset + 3 * NumConversion.BYTES_PER_INT);
  }
    
    /*
     * emprical test to measure benefit of MD5Util.hashCode over ArrayUtil.hashCode. Currently 2x better.
    public static void main(String[] args) {
        int    warmup;
        int    reps;
        int    size;
        byte[][]    checksums;
        MD5Checksum    md5;
        int    sum;
        Stopwatch    sw1;
        Stopwatch    sw2;
        long    md5Elapsed;
        long    arrayElapsed;
        long    md5Average;
        long    arrayAverage;
        
        md5 = new MD5Checksum();
        reps = Integer.parseInt(args[0]);
        warmup = Integer.parseInt(args[1]);
        size = Integer.parseInt(args[2]);
        
        md5Elapsed = 0;
        arrayElapsed = 0;
        
        sum = 0;
        for (int j = 0; j < reps; j++) {
            checksums = new byte[size][];
            for (int i = 0; i < checksums.length; i++) {
                checksums[i] = md5.checksum(NumConversion.intToBytes(i), 0, NumConversion.BYTES_PER_INT); 
            }
            
            sum = 0;
            sw1 = new SimpleStopwatch();
            for (int i = 0; i < checksums.length; i++) {
                sum ^= hashCode(checksums[i]);
            }
            sw1.stop();
            if (j >= warmup) {
                md5Elapsed += sw1.getElapsedNanos();
            }
            System.out.printf("MD5.hashCode      \t%s\n", sw1);
            
            sum = 0;
            sw2 = new SimpleStopwatch();
            for (int i = 0; i < checksums.length; i++) {
                sum ^= ArrayUtil.hashCode(checksums[i]);
            }
            sw2.stop();
            if (j >= warmup) {
                arrayElapsed += sw2.getElapsedNanos();
            }
            System.out.printf("ArrayUtil.hashCode\t%s\n", sw2);            
        }
        
        md5Average = md5Elapsed / (reps - warmup);
        arrayAverage = arrayElapsed / (reps - warmup);
        
        System.out.printf("MD5.hashCode      \t%f\n", ((double)md5Average / 1000000000.0));
        System.out.printf("ArrayUtil.hashCode\t%f\n", ((double)arrayAverage / 1000000000.0));
        
        
        if (sum == 0) {
            System.out.println();
        }
    }
    */
}
