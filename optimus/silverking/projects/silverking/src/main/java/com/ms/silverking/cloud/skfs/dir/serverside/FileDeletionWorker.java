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
package com.ms.silverking.cloud.skfs.dir.serverside;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;

import com.ms.silverking.collection.LightLinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FileDeletionWorker implements Runnable {
  private final BlockingQueue<File> workQueue;

  private static Logger log = LoggerFactory.getLogger(FileDeletionWorker.class);

  private static final String threadName = "FileDeletionWorker";

  public FileDeletionWorker() {
    Thread t;

    workQueue = new LightLinkedBlockingQueue<>();
    t = new Thread(this, threadName);
    t.setDaemon(true);
    t.start();
  }

  @Override
  public void run() {
    while (true) {
      try {
        File f;

        f = workQueue.take();
        if (f != null) {
          if (f.exists() && !f.delete()) {
            log.info("{} unable to delete {}", threadName, f.getAbsolutePath());
          }
        }
      } catch (Exception e) {
        log.error("", e);
      }
    }
  }

  public void delete(File f) {
    try {
      workQueue.put(f);
    } catch (InterruptedException e) {
    }
  }

  public void delete(Collection<File> files) {
    for (File f : files) {
      delete(f);
    }
  }
}
