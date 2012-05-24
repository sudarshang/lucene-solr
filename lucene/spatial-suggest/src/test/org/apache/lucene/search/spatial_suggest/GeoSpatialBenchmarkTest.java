package org.apache.lucene.search.spatial_suggest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.lucene.search.spatial_suggest.Average;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.spatial_suggest.TermFreq;
import org.apache.lucene.search.spatial_suggest.TermFreqArrayIterator;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.RamUsageEstimator;
import org.junit.BeforeClass;

import com.spatial4j.core.shape.simple.RectangleImpl;

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

/*
 * To run this test comment out the Assertions in common-buil.xml and the
 * check for assertions in
 * lucene/test-framework/src/java/org/apache/lucene/util/TestRuleAssertionsRequired.java
 *
 * See below for more details
 * http://lucene.markmail.org/thread/lcx3ymuhiphogq6y
 *
 * This test closely mirrors LookupBenchmarkTest.
 */

public class GeoSpatialBenchmarkTest extends LuceneTestCase{

  static final Charset UTF_8 = Charset.forName("UTF-8");

  private final static int rounds = 15;
  private final static int warmup = 5;

  // store the minLevel, maxLevel pairs that parameterize GeoSpatialLookup
  private final List<List<Integer>> geospatialLookupParams = new ArrayList<List<Integer>>() {{
    add(Arrays.asList(4, 5));
    add(Arrays.asList(4, 6));
  }};

  // number of suggestions to retrieve
  private final int num = 7;

  // this is not used with GeoSpatialSuggest
  private final boolean onlyMorePopular = true;

  // The GeoSpatial shape we are going to restrict suggestions to
  // This is a geobox centers on San Francisco
  RectangleImpl rect = new RectangleImpl(-122.52159118652344,
                                          -122.31697082519531,
                                          37.693601037244406,
                                          37.856422880849514
                                      );


  private final static Random random = new Random(0xdeadbeef);
  /**
   * Input term/weight pairs.
   */
  private static TermFreq [] dictionaryInput;
  /**
   * Benchmark term/weight pairs randomized order
   */
  private static List<TermFreq> benchmarkInput;

  /**
   * Loads terms and frequencies from Wikipedia (cached).
   *
   *
   */
  @BeforeClass
  public static void setup() throws Exception {
    assert false : "disable assertions before running benchmarks!";
    List<TermFreq> input = readSFBiz();
    Collections.shuffle(input, random);
    GeoSpatialBenchmarkTest.dictionaryInput = input.toArray(new TermFreq [input.size()]);
    Collections.shuffle(input, random);
    GeoSpatialBenchmarkTest.benchmarkInput = input;
  }

  /**
   * Collect the businesses for the benchmark
   */
  public static List<TermFreq> readSFBiz() throws Exception {
    List<TermFreq> input = new ArrayList<TermFreq>();
    URL resource = GeoSpatialBenchmarkTest.class.getResource("sf_biz.txt");
    // this assertion will never fire as we run this test with
    // assertions disabled
    assert resource != null : "Resource missing: sf_biz.txt";

    String line = null;
    BufferedReader br = new BufferedReader(new InputStreamReader(resource.openStream(), UTF_8));
    while ((line = br.readLine()) != null) {
      int tab = line.indexOf('\t');
      if (tab < 0) {
        throw new Exception("Invalid Input no tab found " + line);
      }
      int weight = Integer.parseInt(line.substring(tab + 1));
      String key = line.substring(0, tab);
      input.add(new TermFreq(key, weight));
    }
    br.close();
    return input;
  }

  /**
   * Test construction time.
   */
  public void testConstructionTime() throws Exception {
    System.err.println("-- construction time");
    for (final List<Integer> params:geospatialLookupParams) {
      BenchmarkResult result = measure(new Callable<Integer>() {
        public Integer call() throws Exception {
          final Lookup lookup = buildLookup(params.get(0), params.get(1), dictionaryInput);
          return lookup.hashCode();
        }
      });

      System.err.println(
          String.format(Locale.ENGLISH, "%-15s input: %d, time[ms]: %s",
              "minLevel = " + params.get(0) + " maxLevel = " + params.get(1),
              dictionaryInput.length,
              result.average.toString()));
    }
  }

  /**
   * Test memory required for the storage.
   */
  public void testStorageNeeds() throws Exception {
    System.err.println("-- RAM consumption");
    for (final List<Integer> params:geospatialLookupParams) {
      Lookup lookup = buildLookup(params.get(0), params.get(1), dictionaryInput);
      System.err.println(
          String.format(Locale.ENGLISH, "%-15s size[B]:%,13d",
              "minLevel = " + params.get(0) + " maxLevel = " + params.get(1),
              RamUsageEstimator.sizeOf(lookup)));
    }
  }

  /**
   * Create {@link Lookup} instance and populate it.
   */
  private WFSTGeoSpatialLookup buildLookup(int minLevel, int maxLevel, TermFreq[] input) throws Exception {
    WFSTGeoSpatialLookup lookup = new WFSTGeoSpatialLookup(false, minLevel, maxLevel);
    lookup.build(new TermFreqArrayIterator(input));
    return lookup;
  }

  /**
   * Test performance of lookup on full hits.
   */
  public void testPerformanceOnFullHits() throws Exception {
    final int minPrefixLen = 100;
    final int maxPrefixLen = 200;
    runPerformanceTest(minPrefixLen, maxPrefixLen, num, onlyMorePopular);
  }

  /**
   * Test performance of lookup on longer term prefixes (6-9 letters or shorter).
   */
  public void testPerformanceOnPrefixes6_9() throws Exception {
    final int minPrefixLen = 6;
    final int maxPrefixLen = 9;
    runPerformanceTest(minPrefixLen, maxPrefixLen, num, onlyMorePopular);
  }

  /**
   * Test performance of lookup on short term prefixes (2-4 letters or shorter).
   */
  public void testPerformanceOnPrefixes2_4() throws Exception {
    final int minPrefixLen = 2;
    final int maxPrefixLen = 4;
    runPerformanceTest(minPrefixLen, maxPrefixLen, num, onlyMorePopular);
  }

  /**
   * Run the actual benchmark.
   */
  public void runPerformanceTest(final int minPrefixLen, final int maxPrefixLen,
      final int num, final boolean onlyMorePopular) throws Exception {
    System.err.println(String.format(Locale.ENGLISH,
        "-- prefixes: %d-%d, num: %d, onlyMorePopular: %s",
        minPrefixLen, maxPrefixLen, num, onlyMorePopular));

    for (final List<Integer> params:geospatialLookupParams) {
      final WFSTGeoSpatialLookup lookup = buildLookup(params.get(0), params.get(1), dictionaryInput);

      final List<String> input = new ArrayList<String>(benchmarkInput.size());
      for (TermFreq tf : benchmarkInput) {
        String s = tf.term.utf8ToString();
        // our lookup is terminated by the first | character
        s = s.substring(0, s.indexOf('|'));
        input.add(s.substring(0, Math.min(s.length(),
              minPrefixLen + random.nextInt(maxPrefixLen - minPrefixLen + 1))));
      }

      BenchmarkResult result = measure(new Callable<Integer>() {
        public Integer call() throws Exception {
          int v = 0;
          for (String term : input) {
            v += lookup.lookup(term, rect, num).size();
          }
          return v;
        }
      });

      System.err.println(
          String.format(Locale.ENGLISH, "%-15s queries: %d, time[ms]: %s, ~kQPS: %.0f",
              "minLevel = " + params.get(0) + " maxLevel = " + params.get(1),
              input.size(),
              result.average.toString(),
              input.size() / result.average.avg));
    }
  }


  /**
   * Do the measurements.
   */
  private BenchmarkResult measure(Callable<Integer> callable) {
    final double NANOS_PER_MS = 1000000;

    try {
      List<Double> times = new ArrayList<Double>();
      for (int i = 0; i < warmup + rounds; i++) {
          final long start = System.nanoTime();
          guard = callable.call().intValue();
          times.add((System.nanoTime() - start) / NANOS_PER_MS);
      }
      return new BenchmarkResult(times, warmup, rounds);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  /** Guard against opts. */
  @SuppressWarnings("unused")
  private static volatile int guard;

  private static class BenchmarkResult {
    /** Average time per round (ms). */
    public final Average average;

    public BenchmarkResult(List<Double> times, int warmup, int rounds) {
      this.average = Average.from(times.subList(warmup, times.size()));
    }
  }


}
