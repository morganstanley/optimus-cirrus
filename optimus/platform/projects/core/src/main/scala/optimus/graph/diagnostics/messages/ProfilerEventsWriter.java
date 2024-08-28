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
package optimus.graph.diagnostics.messages;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import optimus.graph.OGTrace;
import optimus.graph.OGTraceStore;

public class ProfilerEventsWriter extends ProfilerMessages implements Cloneable {
  private static final byte[] nullByteArray = new byte[0];
  private OGTraceStore.Table table;
  private OGTraceStore store;

  public ProfilerEventsWriter createCopy(OGTraceStore store) {
    try {
      var copy = (ProfilerEventsWriter) clone();
      copy.store = store;
      return copy;
    } catch (CloneNotSupportedException e) {
      e.printStackTrace();
    }
    return null; // shouldn't happen since we implement cloneable
  }

  public final OGTraceStore.Table getTable() {
    return table;
  }

  public final OGTraceStore getStore() {
    return store;
  }

  protected final byte[] getBytes(String s) {
    if (s == null) return nullByteArray;
    return s.getBytes(OGTraceStore.charSet);
  }

  protected final byte[] getBytes(byte[] bytes) {
    return bytes;
  }

  protected final void prefix(int size, int eventId) {
    try {
      table = store.getTableBuffer(table, OGTraceStore.TABLE_EVENTS, size + 4);
    } // +4 for cmd
    catch (Exception e) {
      OGTrace.panic(e, store.backingStoreLocation());
      return;
    }
    table.putInt(eventId); // Add to tracing bp "> " + OGTrace.eventToName(eventId) + " " + table
  }

  protected final void write(int v) {
    table.putInt(v);
  }

  protected final void write(long v) {
    table.putLong(v);
  }

  protected final void write(boolean v) {
    table.putInt(v ? 1 : 0);
  }

  protected final void write(byte[] v) {
    if (v == nullByteArray || v == null) table.putInt(-1); // Negative array size to mean null
    else {
      table.putInt(v.length);
      table.put(v);
    }
  }

  public final void write(OGEvent event) {
    var bos = new ByteArrayOutputStream();
    ObjectOutputStream oos;
    try {
      oos = new ObjectOutputStream(bos);
      oos.writeObject(event);
    } catch (IOException e) {
      e.printStackTrace();
    }
    byte[] data = bos.toByteArray();
    counterEvent(data);
  }

  protected final void suffix() {
    table.setLastCmdPosition();
    if (table.needsThreadName) {
      table.needsThreadName = false;
      var thread = Thread.currentThread();
      threadName(thread.getId(), thread.getName());
    }
  }
}
