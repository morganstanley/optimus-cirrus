/*
 * Copyright 2013-2015 Tom Hombergs (tom.hombergs@gmail.com | http://wickedsource.org)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * |
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/* Modifications to this file were made by Morgan Stanley to write into Groovy and then back into Java.
 * For those changes only:
 *
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
package optimus.git.diffparser;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import optimus.git.diffparser.model.Diff;
import optimus.git.diffparser.model.Hunk;
import optimus.git.diffparser.model.Line;
import optimus.git.diffparser.model.PartialDiff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Every method here must be, otherwise the parser does not work properly.
 */
public enum DiffParser {
  INITIAL {
    @Override
    public DiffParser nextState(ParseWindow window) {
      String line = window.getFocusLine();
      if (isFromFile(line)) {
        return FROM_FILE;
      } else {
        return HEADER;
      }
    }

    @Override
    public PartialDiff modifyDiff(PartialDiff diff, String currentLine) {
      // do nothing
      return diff;
    }
  },
  /**
   * The parser is in this state if it is currently parsing a header line.
   */
  HEADER {
    @Override
    public DiffParser nextState(ParseWindow window) {
      String line = window.getFocusLine();
      if (isFromFile(line)) {
        return FROM_FILE;
      } else if (isEnd(line, window)) {
        return END;
      } else {
        return HEADER;
      }
    }

    @Override
    public PartialDiff modifyDiff(PartialDiff diff, String currentLine) {
      return diff.addHeaderLine(currentLine);
    }
  },
  /**
   * The parser is in this state if it is currently parsing the line containing the "from" file.
   * <p/>
   * Example line:<br/>
   * {@code --- /path/to/file.txt}
   */
  FROM_FILE {
    @Override
    public DiffParser nextState(ParseWindow window) {
      String line = window.getFocusLine();
      if (isToFile(line)) {
        return TO_FILE;
      } else {
        throw new IllegalStateException("A FROM_FILE line ('---') must be directly followed by a TO_FILE line ('+++')!");
      }
    }

    @Override
    public PartialDiff modifyDiff(PartialDiff diff, String currentLine) {
      return diff.setFromFileName(currentLine.replace("--- ", ""));
    }
  },
  /**
   * The parser is in this state if it is currently parsing the line containing the "to" file.
   * <p/>
   * Example line:<br/>
   * {@code +++ /path/to/file.txt}
   */
  TO_FILE {
    @Override
    public DiffParser nextState(ParseWindow window) {
      String line = window.getFocusLine();
      if (Hunk.isHunkStart(line)) {
        return HUNK_START;
      } else {
        throw new IllegalStateException("A TO_FILE line ('+++') must be directly followed by a HUNK_START line ('@@')!");
      }
    }
    @Override
    public PartialDiff modifyDiff(PartialDiff diff, String currentLine) {
      return diff.setToFileName(currentLine.replace("+++ ", ""));
    }
  },
  /**
   * The parser is in this state if it is currently parsing a line containing the header of a hunk.
   * <p/>
   * Example line:<br/>
   * {@code @@ -1,5 +2,6 @@}
   */
  HUNK_START {
    @Override
    public DiffParser nextState(ParseWindow window) {
      String line = window.getFocusLine();
      if (isFromLine(line)) {
        return FROM_LINE;
      } else if (isToLine(line)) {
        return TO_LINE;
      } else {
        return NEUTRAL_LINE;
      }
    }
    @Override
    public PartialDiff modifyDiff(PartialDiff diff, String currentLine) {
      return diff.addHunk(Hunk.fromLine(currentLine));
    }
  },
  /**
   * The parser is in this state if it is currently parsing a line containing a line that is in the first file,
   * but not the second (a "from" line).
   * <p/>
   * Example line:<br/>
   * {@code - only the dash at the start is important}
   */
  FROM_LINE {
    @Override
    public DiffParser nextState(ParseWindow window) {
      String line = window.getFocusLine();
      if (isEnd(line, window)) {
        return END;
      } else if (isFromLine(line)) {
        return FROM_LINE;
      } else if (isToLine(line)) {
        return TO_LINE;
      } else if (Hunk.isHunkStart(line)) {
        return HUNK_START;
      } else {
        return NEUTRAL_LINE;
      }
    }
    @Override
    public PartialDiff modifyDiff(PartialDiff diff, String currentLine) {
      return diff.addLine(Line.from(currentLine.substring(1)));
    }
  },
  /**
   * The parser is in this state if it is currently parsing a line containing a line that is in the second file,
   * but not the first (a "to" line).
   * <p/>
   * Example line:<br/>
   * {@code + only the plus at the start is important}
   */
  TO_LINE {
    @Override
    public DiffParser nextState(ParseWindow window) {
      String line = window.getFocusLine();
      if (isEnd(line, window)) {
        return END;
      } else if (isFromLine(line)) {
        return FROM_LINE;
      } else if (isToLine(line)) {
        return TO_LINE;
      } else if (Hunk.isHunkStart(line)) {
        return HUNK_START;
      } else {
        return NEUTRAL_LINE;
      }
    }

    @Override
    public PartialDiff modifyDiff(PartialDiff diff, String currentLine) {
      return diff.addLine(Line.to(currentLine.substring(1)));
    }
  },
  /**
   * The parser is in this state if it is currently parsing a line that is contained in both files (a "neutral" line).
   * This line can contain any string.
   */
  NEUTRAL_LINE {
    @Override
    public DiffParser nextState(ParseWindow window) {
      String line = window.getFocusLine();
      if (isEnd(line, window)) {
        return END;
      } else if (isFromLine(line)) {
        return FROM_LINE;
      } else if (isToLine(line)) {
        return TO_LINE;
      } else if (Hunk.isHunkStart(line)) {
        return HUNK_START;
      } else {
        return NEUTRAL_LINE;
      }
    }

    @Override
    public PartialDiff modifyDiff(PartialDiff diff, String currentLine) {
      return diff.addLine(Line.neutral(currentLine));
    }
  },
  /**
   * The parser is in this state if it is currently parsing a line that is the delimiter between two Diffs.
   * This line is always a new line.
   */
  END {
    @Override
    public DiffParser nextState(ParseWindow window) {
      return HEADER;
    }

    @Override
    public PartialDiff modifyDiff(PartialDiff diff, String currentLine) {
      // do nothing
      return diff;
    }
  };

  /**
   * Returns the next state of the state machine depending on the current state and the content of a window of lines around the line
   * that is currently being parsed.
   *
   * @param window the window around the line currently being parsed.
   * @return the next state of the state machine.
   */
  protected abstract DiffParser nextState(ParseWindow window);

  protected abstract PartialDiff modifyDiff(PartialDiff diff, String currentLine);

  private static void logTransition(String currentLine, DiffParser fromState, DiffParser toState) {
    logger.debug(String.format("%12s -> %12s: %s", fromState, toState, currentLine));
  }

  public static List<Diff> parse(String text) {
    ParseWindow window = new ParseWindow(new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)));
    DiffParser state = INITIAL;
    List<Diff> parsedDiffs = new ArrayList<>();
    PartialDiff currentDiff = PartialDiff.empty();
    String currentLine;

    while ((currentLine = window.slideForward()) != null) {
      DiffParser newState = state.nextState(window);
      logTransition(currentLine, state, newState);
      currentDiff = newState.modifyDiff(currentDiff, currentLine);
      if (newState.equals(END)) {
        parsedDiffs.add(currentDiff.toDiff());
        currentDiff = PartialDiff.empty();
      }

      state = newState;
    }

    return parsedDiffs;
  }

  private static boolean isFromFile(String line) {
    return line.startsWith("---");
  }

  private static boolean isToFile(String line) {
    return line.startsWith("+++");
  }

  private static boolean isFromLine(String line) {
    return line.startsWith("-");
  }

  private static boolean isToLine(String line) {
    return line.startsWith("+");
  }

  private static boolean isStart(String line) {
    return line.startsWith("diff --git");
  }

  protected static boolean isEnd(String currentLine, ParseWindow window) {
    String nextStartLine = window.getFutureLine(1);
    if (nextStartLine == null) {
      // We reached the end of the stream.
      return true;
    } else if (currentLine.equals("")) {
      // new line, check if the next is new diff
      return isStart(nextStartLine);
    } else if (isStart(nextStartLine)) {
      window.addLine(1, "");
      return false;
    } else {
      return false;
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(DiffParser.class);
}