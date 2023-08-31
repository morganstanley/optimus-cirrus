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

import com.ms.silverking.collection.Triple;
import com.ms.silverking.io.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Convert values stored in StorageValueAndParameters to private mapped byte buffers. */
class SVPMapper {
  private final Mode mode;
  private final FileChannel privateBaseFC;
  private final File fileDir;

  private static Logger log = LoggerFactory.getLogger(SVPMapper.class);

  public enum Mode {
    NoMap,
    PrivateMap,
    FileBackedMap
  };

  public static SVPMapper newPrivateModeMapper(int baseMapSize) throws IOException {
    return new SVPMapper(baseMapSize);
  }

  public static SVPMapper newFileBackedMapper(File fileDir) {
    return new SVPMapper(fileDir);
  }

  private SVPMapper(int baseMapSize) throws IOException {
    mode = Mode.PrivateMap;
    privateBaseFC = createFileChannel(null, baseMapSize).getV1();
    this.fileDir = null;
  }

  private SVPMapper(File fileDir) {
    mode = Mode.FileBackedMap;
    this.fileDir = fileDir;
    this.privateBaseFC = null;
  }

  private Triple<FileChannel, RandomAccessFile, File> createFileChannel(File mapDir, int mapSize)
      throws IOException {
    File f;
    RandomAccessFile rf;
    FileChannel fc;

    rf = null;
    f = File.createTempFile("SVPMapper", ".map", mapDir);
    f.deleteOnExit();
    FileUtil.writeToFile(f, new byte[mapSize]);
    rf = new RandomAccessFile(f, "rw");
    fc = rf.getChannel();
    return new Triple<>(fc, rf, f);
  }

  private MappedByteBuffer createMap(int size) throws IOException {
    MappedByteBuffer mbb;

    switch (mode) {
      case PrivateMap:
        mbb = privateBaseFC.map(MapMode.PRIVATE, 0, size);
        break;
      case FileBackedMap:
        Triple<FileChannel, RandomAccessFile, File> f;

        f = null;
        try {

          f = createFileChannel(fileDir, size);
          mbb = f.getV1().map(MapMode.READ_WRITE, 0, size);
        } finally {
          try {
            f.getV2().close();
          } finally {
            f.getV3().delete();
          }
        }
        break;
      default:
        throw new RuntimeException("panic");
    }
    return mbb;
  }

  public StorageValueAndParameters convertToMappedSVP(StorageValueAndParameters svp)
      throws IOException {
    StorageValueAndParameters msvp;

    msvp =
        new StorageValueAndParameters(
            svp.getKey(),
            convertToMappedBuffer(svp.getValue()),
            svp.getVersion(),
            svp.getUncompressedSize(),
            svp.getCompressedSize(),
            svp.getCCSS(),
            svp.getChecksum(),
            svp.getValueCreator(),
            svp.getCreationTime(),
            svp.getRequiredPreviousVersion(),
            svp.getLockSeconds());
    return msvp;
  }

  private ByteBuffer convertToMappedBuffer(ByteBuffer b) throws IOException {
    ByteBuffer mb;

    log.info("convertToMappedBuffer {}\n", b.remaining());
    mb = createMap(b.duplicate().remaining());

    mb.put(b);
    mb.flip();
    return mb;
  }
}
