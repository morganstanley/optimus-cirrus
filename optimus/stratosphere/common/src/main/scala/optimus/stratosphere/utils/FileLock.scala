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
package optimus.stratosphere.utils

import java.io.File
import java.io.RandomAccessFile
import java.nio.channels.OverlappingFileLockException
import java.nio.channels.{FileLock => JFileLock}

/**
 * @param file
 *   File which we are using to synchronize
 *
 * To work properly every instance of FileLock for given file should get the same File instance as a parameter
 */
class FileLock(file: File) {

  def locked[T](codeBlock: => T): T = file.synchronized {
    val channel = new RandomAccessFile(file, "rw").getChannel
    var fileLock: JFileLock = null

    try {
      file.createNewFile()

      def tryLock(): Boolean = {
        this synchronized {
          try {
            // This method will return null if the lock is held by another process,
            // but will throw an exception if the lock is held by this process so we have to handle both cases
            fileLock = channel.tryLock()
            fileLock != null
          } catch {
            case e: OverlappingFileLockException => false
          }
        }
      }

      while (!tryLock()) Thread.sleep(100)

      codeBlock
    } finally {
      if (fileLock != null) fileLock.release()
      channel.close()
    }
  }
}
