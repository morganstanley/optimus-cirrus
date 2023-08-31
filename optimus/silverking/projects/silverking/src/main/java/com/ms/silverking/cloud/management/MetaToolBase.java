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
package com.ms.silverking.cloud.management;

import java.io.File;
import java.io.IOException;

import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

public abstract class MetaToolBase {

  // FUTURE - all silverking meta data tooling really needs a redesign as the tooling has
  // moved well beyond the original design

  public MetaToolBase() {}

  protected abstract void doWork(MetaToolOptions options) throws IOException, KeeperException;

  public static <T> void doWork(MetaToolOptions options, MetaToolWorker<T> worker)
      throws IOException, KeeperException {
    long version;

    version = Long.parseLong(options.version);
    worker.write(options, version, worker.read(options, version));
  }

  public static class MetaToolWorker<T> {
    private final MetaToolModule<T> module;

    public MetaToolWorker(MetaToolModule<T> module) {
      this.module = module;
    }

    private T read(MetaToolOptions options, long version) throws IOException, KeeperException {
      MetaToolSource source;

      source = MetaToolSource.valueOf(options.source);
      switch (source) {
        case FILE:
          return module.readFromFile(new File(options.file), version);
        case ZOOKEEPER:
          try {
            if (version < 0) {
              version = module.getLatestVersion();
            }
            return module.readFromZK(version, options);
          } catch (Exception ex) {
            throw KeeperException.forMethod("MetaToolModule::readFromZK", ex);
          }
        default:
          throw new RuntimeException("panic");
      }
    }

    private void write(MetaToolOptions options, long version, T instance)
        throws IOException, KeeperException {
      MetaToolDest dest;

      dest = MetaToolDest.valueOf(options.dest);
      switch (dest) {
        case FILE:
          module.writeToFile(new File(options.file), instance);
          break;
        case ZOOKEEPER:
          try {
            module.writeToZK(instance, options);
          } catch (Exception ex) {
            throw KeeperException.forMethod("MetaToolModule::writeToZK", ex);
          }
          break;
        case STDOUT:
          System.out.println(instance);
          break;
        default:
          throw new RuntimeException("panic");
      }
    }
  }

  /**
   * @param args
   */
  public void runTool(String[] args) {
    try {
      MetaToolOptions options;
      CmdLineParser parser;

      options = new MetaToolOptions();
      parser = new CmdLineParser(options);
      try {
        parser.parseArgument(args);
        doWork(options);
      } catch (CmdLineException cle) {
        System.err.println(cle.getMessage());
        parser.printUsage(System.err);
        return;
      } catch (Exception e) {
        e.printStackTrace();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // concrete MetaTool main should look like:
  // public static void main(String[] args) {
  //    new MetaTool().runTool(args);
  // }
}
