package org.apache.lucene.search.spatial_suggest;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;


import org.apache.lucene.search.spell.TermFreqIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.search.suggest.fst.Sort;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.Node;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PairOutputs;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.apache.lucene.util.fst.PairOutputs.Pair;
import org.apache.lucene.util.fst.Util.MinResult;

import com.spatial4j.core.context.simple.SimpleSpatialContext;
import com.spatial4j.core.distance.DistanceUnits;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import com.spatial4j.core.util.GeohashUtils;

/**
 * Suggester based on WFST:
 * Uses geohashes as a prefix to the input during the build and lookup stages to restrict suggestions
 * to a particular geographical area.
 *
 */
public class WFSTGeoSpatialLookup extends Lookup {

  private static String SEPERATOR = "|";
  private static byte[] BYTE_SEPERATOR = {0,0};

  /**
  * GEOHASH_KEY_SEPERATOR UTF-8 bytes for '|' character
  */
  // any character that is not among the geohash prefix
  // characters works as a separator.
  private static byte[] GEOHASH_KEY_SEPERATOR = "|".getBytes(Charset.forName("UTF-8"));

  /**
   * minLevel:
   * length of the most imprecise (largest) geohash prefix
   */
  private final int minLevel;

  /**
   * maxLevel:
   * length of the most precise (smallest) geohash prefix
   */
  private final int maxLevel;

  /**
   * FST<Weight,Display>:
   *  input is the suggestion input with the geohash prefix
   *  weights are encoded as costs: (Integer.MAX_VALUE-weight)
   *  The display is the suggestion to be displayed.
   */

  //               weight, display
  private FST<Pair<Long,BytesRef>> fst = null;

  // setting this to True will ensure that
  // an exact match (if one exists) will be the first suggestion
  private final boolean exactFirst;

  /**
   * Creates a new geospatial suggester
   * @param exactFirst <code>true</code> if suggestions that match the
   *        prefix exactly should always be returned first, regardless
   *        of score. This has no performance impact, but could result
   *        in low-quality suggestions.
   * @param minLevel geohash level for the most imprecise geohashes
   * @param maxLevel geohash level for the most precise geohashes
   */
  public WFSTGeoSpatialLookup(boolean exactFirst, int minLevel, int maxLevel) {
    this.exactFirst = exactFirst;
    this.minLevel = minLevel;
    this.maxLevel = maxLevel;
  }

  /*
   * Builds the fst from the suggestion input.
   * Expects the lookup returned by the iterator to have
   * the following format
   *
   * suggestion|SuggestionDisplay|latitude|longitude
   *
   * This means that the original format in file would need to be
   * suggestion|SuggestionDisplay|longitude|latitude\tweight as
   * ther TermFreqIterator splits on the TAB character
   *
   */
  @Override
  public void build(TermFreqIterator iterator) throws IOException {

    // setup the directories and file for sorting
    String prefix = getClass().getSimpleName();
    File directory = Sort.defaultTempDir();
    File tempInput = File.createTempFile(prefix, ".input", directory);
    File tempSorted = File.createTempFile(prefix, ".sorted", directory);

    Sort.ByteSequencesWriter writer = new Sort.ByteSequencesWriter(tempInput);
    Sort.ByteSequencesReader reader = null;

    boolean success = false;
    byte buffer[] = new byte[20];

    try {
      ByteArrayDataOutput output = new ByteArrayDataOutput(buffer);
      BytesRef suggestInputTerm;
      while ((suggestInputTerm = iterator.next()) != null) {
        // input has the following format
        // suggestion|SuggestionDisplay|latitude|longitude  weight
        // compute the required length of buffer:
        // spare.length - 3 (pipes) + 2 (separator) + weight (4) + MAX_LEVEL

        int requiredLength = suggestInputTerm.length - 3 + 2 + 4 + this.maxLevel;
        buffer = ArrayUtil.grow(buffer, requiredLength);

        byte separatorByte = SEPERATOR.getBytes("UTF8")[0];

        int suggestSeperatorPos = indexOf(suggestInputTerm.bytes,
                                          separatorByte,
                                          0,
                                          suggestInputTerm.length);

        int displaySeperatorPos = indexOf(suggestInputTerm.bytes,
                                          separatorByte,
                                          suggestSeperatorPos + 1,
                                          suggestInputTerm.length);

        int latitudeSeperatorPos = indexOf(suggestInputTerm.bytes,
                                            separatorByte,
                                            displaySeperatorPos + 1,
                                            suggestInputTerm.length);

        Double latitude = Double.parseDouble(new String(suggestInputTerm.bytes,
                                                         displaySeperatorPos + 1,
                                                         latitudeSeperatorPos - (displaySeperatorPos + 1))
                                                   );
        Double longitude = Double.parseDouble(new String(suggestInputTerm.bytes,
                                                        latitudeSeperatorPos + 1,
                                                        suggestInputTerm.length - (latitudeSeperatorPos + 1))
                                                        );
        // combine the lookup string
        for (int level = minLevel; level <= maxLevel; level++) {
          output.reset(buffer);
          byte[] geohash = GeohashUtils.encodeLatLon(latitude, longitude, level).getBytes("UTF8");
          output.writeBytes(geohash, 0, geohash.length);
          output.writeBytes(GEOHASH_KEY_SEPERATOR, 0, GEOHASH_KEY_SEPERATOR.length);
          output.writeBytes(suggestInputTerm.bytes, 0, suggestSeperatorPos);
          output.writeByte((byte)0); // separator: not used, just for sort order
          output.writeByte((byte)0); // separator: not used, just for sort order
          output.writeInt((int)encodeWeight(iterator.weight()));
          output.writeBytes(suggestInputTerm.bytes, suggestSeperatorPos + 1,
                              displaySeperatorPos - (suggestSeperatorPos + 1));
          writer.write(buffer, 0, output.getPosition());
        }
      }

      writer.close();
      new Sort().sort(tempInput, tempSorted);
      reader = new Sort.ByteSequencesReader(tempSorted);

      // so now we have combined the input with the geohashes and sorted it
      // we now need to remove duplicates ie multiple Starbucks in the
      // area of a geohash. If we have duplicates in the input we store a single
      // entry in the FST with the lowest cost

      PairOutputs<Long,BytesRef> outputs = new PairOutputs<Long,BytesRef>(PositiveIntOutputs.getSingleton(true), ByteSequenceOutputs.getSingleton());
      Builder<Pair<Long,BytesRef>> builder = new Builder<Pair<Long,BytesRef>>(FST.INPUT_TYPE.BYTE1, outputs);

      // setup locals to store first entry
      BytesRef suggestInput = new BytesRef();
      BytesRef suggestDisplay = new BytesRef();
      IntsRef scratchInts = new IntsRef();
      ByteArrayDataInput input = new ByteArrayDataInput();

      // setup locals to store next entry
      BytesRef nextSuggestInput = new BytesRef();
      BytesRef nextSuggestDisplay = new BytesRef();

      BytesRef nextScratch = new BytesRef();

      int separatorPos;
      long cost = 0;
      BytesRef scratch = new BytesRef();
      // store the first entry
      if (reader.read(scratch)) {
        input.reset(scratch.bytes, scratch.offset, scratch.length);
        separatorPos = indexOf(scratch.bytes, BYTE_SEPERATOR);
        suggestInput.bytes = scratch.bytes;
        suggestInput.offset = scratch.offset;
        suggestInput.length = separatorPos;

        input.setPosition(separatorPos + 2); // suggestDisplay + separator
        cost = input.readInt();

        suggestDisplay.bytes = scratch.bytes;
        suggestDisplay.offset = input.getPosition();
        suggestDisplay.length = input.length() - input.getPosition();
      }

      while (reader.read(nextScratch)) {
        // get the next entry
        input.reset(nextScratch.bytes, nextScratch.offset, nextScratch.length);

        separatorPos = indexOf(nextScratch.bytes, BYTE_SEPERATOR);
        nextSuggestInput.bytes = nextScratch.bytes;
        nextSuggestInput.offset = nextScratch.offset;
        nextSuggestInput.length = separatorPos;
        input.setPosition(separatorPos + 2); // suggestDisplay + separator
        long nextCost = input.readInt();

        nextSuggestDisplay.bytes = nextScratch.bytes;
        nextSuggestDisplay.offset = input.getPosition();
        nextSuggestDisplay.length = input.length() - input.getPosition();

        if (!nextSuggestInput.bytesEquals(suggestInput)) {
          // next entry is not a duplicate so add previous entry to the fst
          Util.toIntsRef(suggestInput, scratchInts);
          builder.add(scratchInts, outputs.newPair(cost, BytesRef.deepCopyOf(suggestDisplay)));

          // copy over next entry to previous entry
          cost = nextCost;
          suggestInput = BytesRef.deepCopyOf(nextSuggestInput);
          suggestDisplay = BytesRef.deepCopyOf(nextSuggestDisplay);
        } else {
          // we have a duplicate set the cost to the minimum of the 2 costs
          cost = Math.min(cost, nextCost);
        }
      }

      Util.toIntsRef(suggestInput, scratchInts);

      builder.add(scratchInts, outputs.newPair(cost, BytesRef.deepCopyOf(suggestDisplay)));

      fst = builder.finish();

      success = true;
    } finally {
      if (success) {
        IOUtils.close(reader, writer);
      } else {
        IOUtils.closeWhileHandlingException(reader, writer);
      }

      tempInput.delete();
      tempSorted.delete();
    }

  }

  /**
   * Use prefix as input to walk to a node in an fst
   * @param prefix
   * @param arc - contains the initial node to start the walk from and is modified to point to the
   *              destination of the walk
   * @return - output collected till the end node
   * @throws IOException
   */
  private Pair<Long,BytesRef> lookupPrefix(BytesRef prefix, Arc<Pair<Long,BytesRef>> arc) throws /*Bogus*/IOException {
    Pair<Long,BytesRef> output = fst.outputs.getNoOutput();

    BytesReader bytesReader = fst.getBytesReader(0);

    fst.getFirstArc(arc);

    byte[] bytes = prefix.bytes;
    int pos = prefix.offset;
    int end = pos + prefix.length;
    while (pos < end) {
      if (fst.findTargetArc(bytes[pos++] & 0xff, arc, arc, bytesReader) == null) {
        return null;
      } else {
        output = fst.outputs.add(output, arc.output);
      }
    }

    return output;
  }

  /**
   * Return top 'num' suggestions with key as the prefix and restricted to the geographical
   * area under shape.
   *
   * Note:- because results are returned from all the geohashes that are either inside or intersect
   * the shape, some of the results returned might be outside the shape.
   * @param key
   *         prefix for the suggestions
   * @param shape
   *          geographical shape by which suggestions are restricted
   * @param num
   *          maximum number of suggestions to return
   * @return
   */
  public List<LookupResult> lookup(CharSequence key, Shape shape,
      int num) {
    assert num > 0;

    List<Node> hashesForShape = getHashesForShape(shape);
    List<LookupResult> results = new ArrayList<LookupResult>(num);
    BytesRef keyBytes = new BytesRef(key);

    BytesRef scratch = new BytesRef(this.maxLevel +
        keyBytes.length +
        GEOHASH_KEY_SEPERATOR.length);
    CharsRef spare = new CharsRef();

    LookupResult exactFirstResult = null;
    Util.TopNSearcher<Pair<Long,BytesRef>> searcher = new Util.TopNSearcher<Pair<Long,BytesRef>>(fst, num, weightComparator);

    // for each of the geohash contained by our shape
    // we append the key and get the fst node corresponding
    // to that input. The nodes are queued up as seeds for
    // the searcher.
    for (Node geoHash:hashesForShape) {
      try {
        byte[] hashBytes = geoHash.getTokenBytes();

        // setup the prefix for lookup
        System.arraycopy(hashBytes, 0, scratch.bytes, 0, hashBytes.length);
        System.arraycopy(GEOHASH_KEY_SEPERATOR, 0, scratch.bytes,
                                        hashBytes.length, GEOHASH_KEY_SEPERATOR.length);
        System.arraycopy(keyBytes.bytes, 0, scratch.bytes,
            hashBytes.length + GEOHASH_KEY_SEPERATOR.length, keyBytes.length);
        scratch.offset = 0;
        scratch.length = hashBytes.length + GEOHASH_KEY_SEPERATOR.length + keyBytes.length;

        Arc<Pair<Long,BytesRef>> arc = new Arc<Pair<Long,BytesRef>>();
        Pair<Long,BytesRef> prefixOutput;

        prefixOutput = lookupPrefix(scratch, arc);
        // no prefixOutput indicates we did not
        // find anything for the current geohash key combination
        if (prefixOutput == null) {
          continue;
        }

        if (exactFirst && arc.isFinal()) {
          BytesRef prefix = BytesRef.deepCopyOf(prefixOutput.output2);

          prefix.append(arc.nextFinalOutput.output2);
          spare.grow(prefix.length);
          UnicodeUtil.UTF8toUTF16(prefix, spare);
          int currentWeight = decodeWeight(prefixOutput.output1 + arc.nextFinalOutput.output1);
          if (exactFirstResult == null) {
            exactFirstResult = new LookupResult(spare.toString(),
                                      currentWeight);
          } else if (currentWeight > exactFirstResult.value) {
            exactFirstResult = new LookupResult(spare.toString(),
                currentWeight);
          }
        }

        IntsRef inputPath = new IntsRef();
        Util.toIntsRef(scratch, inputPath);

        searcher.addStartPaths(arc, prefixOutput, inputPath, !exactFirst);
      } catch (IOException bogus) { throw new RuntimeException(bogus); }
    }

    MinResult<Pair<Long,BytesRef>> completions[] = null;
    try {
      completions = searcher.search();
    } catch (IOException bogus) {
      throw new RuntimeException(bogus);
    }

    if (exactFirstResult != null) {
      results.add(exactFirstResult);
      if (results.size() == num) {
        return results;
      }
    }

    for (MinResult<Pair<Long,BytesRef>> completion : completions) {
      spare.grow(completion.output.output2.length);
      UnicodeUtil.UTF8toUTF16(completion.output.output2, spare);
      results.add(new LookupResult(spare.toString(), decodeWeight(completion.output.output1)));
      // this check is necessary as we could have added an exact match to results
      if (results.size() == num) {
        break;
      }
    }

    return results;
  }

  /**
   * Returns a list of Nodes (geohashes) that are within the specified minLevel and maxLevels
   * and completely cover the shape
   * @param shape
   * @return
   */
  private List<Node> getHashesForShape(Shape shape) {
    GeohashPrefixTree grid = new GeohashPrefixTree(new SimpleSpatialContext(DistanceUnits.MILES),
        GeohashUtils.MAX_PRECISION);

    LinkedList<Node> cells =  new LinkedList<Node>(grid.getWorldNode().getSubCells(shape));

    List<Node> result = new ArrayList<Node>();

    while(!cells.isEmpty()) {
      final Node cell = cells.removeFirst();

      if (cell.getShapeRel() == SpatialRelation.WITHIN && cell.getLevel() > minLevel) {
        // if the geohash is contained in the shape add it
        result.add(cell);
      } else if (cell.getLevel() == maxLevel || cell.isLeaf()) {
        // either the shape is within the geohash or they intersect
        // if we have reached the precision we desire add the cell
        result.add(cell);
      } else {
        // get subcells the are not disjoint from our shape
        cells.addAll(0, cell.getSubCells(shape));
      }
    }
    return result;
  }
  @Override
  public boolean store(OutputStream output) throws IOException {
    try {
      fst.save(new OutputStreamDataOutput(output));
    } finally {
      IOUtils.close(output);
    }
    return true;
  }

  @Override
  public boolean load(InputStream input) throws IOException {
    try {
      this.fst = new FST<Pair<Long,BytesRef>>(new InputStreamDataInput(input), new PairOutputs<Long,BytesRef>(PositiveIntOutputs.getSingleton(true), ByteSequenceOutputs.getSingleton()));
    } finally {
      IOUtils.close(input);
    }
    return true;
  }

  /** cost -> weight */
  private static int decodeWeight(long encoded) {
    return (int)(Integer.MAX_VALUE - encoded);
  }

  /** weight -> cost */
  private static int encodeWeight(long value) {
    if (value < 0 || value > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("cannot encode value: " + value);
    }
    return Integer.MAX_VALUE - (int)value;
  }

  static final Comparator<Pair<Long,BytesRef>> weightComparator = new Comparator<Pair<Long,BytesRef>> () {
    public int compare(Pair<Long,BytesRef> left, Pair<Long,BytesRef> right) {
      return left.output1.compareTo(right.output1);
    }
  };

  // copy pasted from Gauva Bytes class
  private static int indexOf(
      byte[] array, byte target, int start, int end) {
    for (int i = start; i < end; i++) {
      if (array[i] == target) {
        return i;
      }
    }
    return -1;
  }

  // copy passted from Gauva Bytes class
  // nocommit need to find the right home for the following two methods
  /**
   * Returns the start position of the first occurrence of the specified {@code
   * target} within {@code array}, or {@code -1} if there is no such occurrence.
   *
   * <p>More formally, returns the lowest index {@code i} such that {@code
   * java.util.Arrays.copyOfRange(array, i, i + target.length)} contains exactly
   * the same elements as {@code target}.
   *
   * @param array the array to search for the sequence {@code target}
   * @param target the array to search for as a sub-sequence of {@code array}
   */
  public static int indexOf(byte[] array, byte[] target) {
    if (target.length == 0) {
      return 0;
    }

    outer:
    for (int i = 0; i < array.length - target.length + 1; i++) {
      for (int j = 0; j < target.length; j++) {
        if (array[i + j] != target[j]) {
          continue outer;
        }
      }
      return i;
    }
    return -1;
  }

  @Override
  public List<LookupResult> lookup(CharSequence key, boolean onlyMorePopular,
      int num) {
    throw new UnsupportedOperationException("Must provide a Shape for GeoSpatial Suggest");
  }
}
