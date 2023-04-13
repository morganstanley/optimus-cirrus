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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.io.FileUtil;
import com.ms.silverking.thread.ThreadUtil;

public class SVPConversionTest {
  private FileChannel fc;
  private BlockingQueue<StorageValueAndParameters> q;

  public SVPConversionTest() throws IOException {
    createBaseMap(100000);
    q = new LinkedBlockingQueue<>();
  }

  private void createBaseMap(int baseMapSize) throws IOException {
    File f;
    RandomAccessFile rf;

    f = File.createTempFile("svp", ".map");
    f.deleteOnExit();
    System.out.printf("TempFile %s\n", f);
    FileUtil.writeToFile(f, new byte[baseMapSize]);
    rf = new RandomAccessFile(f, "rw");
    fc = rf.getChannel();
  }

  private MappedByteBuffer createMap(int size) throws IOException {
    MappedByteBuffer mbb;

    mbb = fc.map(MapMode.PRIVATE, 0, size);
    return mbb;
  }

  private StorageValueAndParameters convertToMappedSVP(StorageValueAndParameters svp) throws IOException {
    StorageValueAndParameters msvp;

    msvp = new StorageValueAndParameters(svp.getKey(), convertToMappedBuffer(svp.getValue()), svp.getVersion(),
        svp.getUncompressedSize(), svp.getCompressedSize(), svp.getCCSS(), svp.getChecksum(), svp.getValueCreator(),
        svp.getCreationTime(), svp.getRequiredPreviousVersion(), svp.getLockSeconds());
    return msvp;
  }

  private ByteBuffer convertToMappedBuffer(ByteBuffer b) throws IOException {
    ByteBuffer mb;

    mb = createMap(b.remaining());
    mb.put(b);
    return mb;
  }

  public void push(StorageValueAndParameters svp) throws IOException {
    try {
      q.put(convertToMappedSVP(svp));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public StorageValueAndParameters pull() {
    try {
      return q.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public StorageValueAndParameters createSVP(int valueSize) {
    return new StorageValueAndParameters(new SimpleKey(1, 2), ByteBuffer.allocate(valueSize), 10, valueSize, valueSize,
        (short) 0, new byte[4], new byte[ValueCreator.BYTES], System.currentTimeMillis(), 0, (short) 0);
  }

  public void runTest(int valueSize, int numValues) throws IOException {
    for (int i = 0; i < numValues; i++) {
      System.out.printf("Push %d\n", i);
      push(createSVP(valueSize));
    }
    for (int i = 0; i < numValues; i++) {
      StorageValueAndParameters svp;

      svp = pull();
      System.out.printf("Pulled %d %s\n", i, svp);
    }
  }

  public static void main(String[] args) {
    try {
      SVPConversionTest t;

      t = new SVPConversionTest();
      t.runTest(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
      ThreadUtil.sleepSeconds(Integer.parseInt(args[2]));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
