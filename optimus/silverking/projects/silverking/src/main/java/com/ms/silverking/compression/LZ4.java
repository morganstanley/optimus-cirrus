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
package com.ms.silverking.compression;

import java.io.File;
import java.io.IOException;

import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.io.FileUtil;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class LZ4 implements Compressor, Decompressor {
  private static final LZ4Factory factory = LZ4Factory.fastestInstance();

  private static Logger log = LoggerFactory.getLogger(LZ4.class);

  public LZ4() {
  }

  public byte[] compress(byte[] rawValue, int offset, int length) throws IOException {
    LZ4Compressor compressor;
    int maxCompressedLength;
    byte[] compressed;
    int compressedLength;
    byte[] buf;

    compressor = factory.fastCompressor();
    maxCompressedLength = compressor.maxCompressedLength(length);
    compressed = new byte[maxCompressedLength];
    compressedLength = compressor.compress(rawValue, 0, length, compressed, 0, maxCompressedLength);

    buf = new byte[compressedLength];
    System.arraycopy(compressed, 0, buf, 0, compressedLength);
    // FUTURE - eliminate the copy
    return buf;
  }

  public byte[] decompress(byte[] value, int offset, int length, int uncompressedLength) throws IOException {
    LZ4FastDecompressor decompressor;
    byte[] restored;

    decompressor = factory.fastDecompressor();
    restored = new byte[uncompressedLength];
    decompressor.decompress(value, offset, restored, 0, uncompressedLength);
    return restored;
  }

  private static final Pair<Triple<Double, Double, Double>, Pair<Integer, Integer>> compressFile(File file)
      throws IOException {
    Stopwatch readingSW;
    Stopwatch compressionSW;
    Stopwatch decompressionSW;
    byte[] original;
    byte[] compressed;
    byte[] uncompressed;

    readingSW = new SimpleStopwatch();
    log.info("Reading file:          {}", file);
    original = FileUtil.readFileAsBytes(file);
    readingSW.stop();
    log.info("Reading elapsed:       {}", readingSW.getElapsedSeconds());
    compressionSW = new SimpleStopwatch();
    compressed = new LZ4().compress(original, 0, original.length);
    compressionSW.stop();
    log.info("Compression elapsed:   {}", compressionSW.getElapsedSeconds());
    decompressionSW = new SimpleStopwatch();
    uncompressed = new LZ4().decompress(compressed, 0, compressed.length, original.length);
    decompressionSW.stop();
    log.info("Decompression elapsed: {}", decompressionSW.getElapsedSeconds());
    return new Pair<>(new Triple<>(readingSW.getElapsedSeconds(), compressionSW.getElapsedSeconds(),
        decompressionSW.getElapsedSeconds()), new Pair<Integer, Integer>(original.length, compressed.length));
  }

  private static final Pair<Triple<Double, Double, Double>, Pair<Integer, Integer>> compressFile(String fileName)
      throws IOException {
    return compressFile(new File(fileName));
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      Triple<Double, Double, Double> sum;
      long totalOriginal;
      long totalCompressed;

      sum = new Triple<>(0.0, 0.0, 0.0);
      totalOriginal = 0;
      totalCompressed = 0;
      for (String arg : args) {
        Pair<Triple<Double, Double, Double>, Pair<Integer, Integer>> result;
        Triple<Double, Double, Double> t;

        result = compressFile(arg);
        t = result.getV1();
        sum = new Triple<>(sum.getV1() + t.getV1(), sum.getV2() + t.getV2(), sum.getV3() + t.getV3());
        totalOriginal += result.getV2().getV1();
        totalCompressed += result.getV2().getV2();
      }
      if (args.length > 1) {
        log.info("Total:");
        log.info("Reading elapsed:       {}", sum.getV1());
        log.info("Compression elapsed:   {}", sum.getV2());
        log.info("Decompression elapsed: {}", sum.getV3());
        log.info("Original bytes:        {}", totalOriginal);
        log.info("Compressed bytes:      {}", totalCompressed);
        log.info("Compressed / original: {}", (double) totalCompressed / (double) totalOriginal);
      }
            /*
            for (String arg : args) {
                byte[]  original;
                byte[]  compressed;
                byte[]  uncompressed;
                original = arg.getBytes();
                compressed = new LZ4().compress(original, 0, original.length);
                uncompressed = new LZ4().decompress(compressed, 0, compressed.length, original.length);
                //print of uncompressed may be 'corrupted' by non-printable chars from MD5  
                System.out.println(arg +"\t"+ original.length +"\t"+ compressed.length +"\t"+ new String(uncompressed));
                System.out.println(StringUtil.byteArrayToHexString(original));
                System.out.println(StringUtil.byteArrayToHexString(uncompressed));
                //int len = uncompressed.length - MD5Hash.MD5_BYTES;
                //byte[] noMd5 = new byte[len];  
                //System.arraycopy(uncompressed, 0, noMd5, 0, len);
                //System.out.println(StringUtil.byteArrayToHexString(noMd5));
            }
            */
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
