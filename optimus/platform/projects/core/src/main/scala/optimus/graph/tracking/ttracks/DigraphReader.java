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
package optimus.graph.tracking.ttracks;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

import optimus.graph.AlreadyCompletedPropertyNode;
import optimus.graph.NodeTask;
import optimus.graph.NodeTaskInfo;
import optimus.graph.PropertyInfo;
import optimus.graph.TweakableKey;

final class DigraphReader {
  private StreamTokenizer st;
  private HashMap<String, TTrack> all = new HashMap<>();
  // Keep fake tasks around to avoid GC removing them
  private ArrayList<NodeTask> allTasks = new ArrayList<>();
  // Linked to provide ordering guarantees
  LinkedHashMap<TweakableKey, TTrackRoot> roots = new LinkedHashMap<>();

  DigraphReader(String text) throws IOException {
    st = new StreamTokenizer(new StringReader(text));
    read();
  }

  private void eatTo(int ttype) throws IOException {
    while (st.nextToken() != StreamTokenizer.TT_EOF) {
      if (st.ttype == ttype || st.ttype == '\n') break;
    }
  }

  private void read() throws IOException {
    st.nextToken();
    st.eolIsSignificant(true);
    st.whitespaceChars('-', '-');
    if (st.ttype != StreamTokenizer.TT_WORD || !"digraph".equals(st.sval))
      throw new IllegalArgumentException("Header?");
    st.nextToken();
    if (st.ttype != '{') throw new IllegalArgumentException("Expected '{'");

    loop:
    while (st.nextToken() != StreamTokenizer.TT_EOF) {
      if (st.ttype == '{') eatTo('}'); // Eat rank info....
      else if (st.ttype == StreamTokenizer.TT_WORD) {
        if ("}".equals(st.sval)) break loop; // End sub-graph
        if ("graph".equals(st.sval)) { // Graph parameters
          eatTo(';'); // Ignore the attributes...
          continue;
        }
        String name = st.sval; // Node name
        st.nextToken();
        if (st.ttype == '[') { // Read attributes
          st.nextToken();
          String attr = st.sval;
          if (!"label".equals(attr)) {
            eatTo(']');
            continue;
          }
          st.nextToken();
          if (st.ttype != '=') throw new IllegalArgumentException("Expected '='");
          st.nextToken();
          String attrVal = st.sval; // propertyInfo name
          String shape = "";
          st.nextToken();
          if (st.ttype == StreamTokenizer.TT_WORD && "shape".equals(st.sval)) {
            st.nextToken(); // '='
            st.nextToken(); // shape name
            shape = st.sval;
          }
          String colour = "";
          st.nextToken();
          if (st.ttype == StreamTokenizer.TT_WORD && "color".equals(st.sval)) {
            st.nextToken(); // '='
            st.nextToken(); // colour name
            colour = st.sval;
          } else st.pushBack();

          TTrack ttrack = new TTrack();
          // if attrVal == Nil_NAME, there is no TweakableKey in ttracks map and ttrack.nodes ==
          // TTrackRef.Nil
          if (!DigraphWriter.NIL_NAME.equals(attrVal)) {
            PropertyInfo<String> pinfo = new PropertyInfo<>(attrVal, 0, null);
            if (shape.equals(
                DigraphWriter.ROOT_SHAPE)) { // ie this was a TTrackRoot in the ttracks map
              TTrackRoot root = new TTrackRoot();
              TweakableKey key =
                  new TweakableKey() {
                    public NodeTaskInfo propertyInfo() {
                      return pinfo;
                    }

                    public TweakableKey tidyKey() {
                      return this;
                    }

                    @Override
                    public TweakableKey trackKey() {
                      return this;
                    }
                  };
              roots.put(key, root);
              ttrack = root;
            }
            NodeTask ntsk = new AlreadyCompletedPropertyNode<String>(null, null, pinfo);
            ttrack.nodes = new TTrackRef(ntsk, null, null);
            allTasks.add(ntsk);
          }
          if (colour.equals(DigraphWriter.NULL_COLOUR) || ttrack.nodes == null)
            ttrack.nodes = TTrackRef.Nil;
          all.put(name, ttrack);
          eatTo(']');
        } else if (st.ttype == '>') { // Read edge
          st.nextToken();
          String edgeTo = st.sval;
          TTrack from = all.get(name);
          TTrack to = all.get(edgeTo);
          from.addCaller(to, 0);
        }
      }
    }
  }
}
