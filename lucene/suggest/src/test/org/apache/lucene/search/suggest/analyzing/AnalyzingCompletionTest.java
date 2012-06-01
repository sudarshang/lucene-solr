package org.apache.lucene.search.suggest.analyzing;

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

import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.search.suggest.TermFreq;
import org.apache.lucene.search.suggest.TermFreqArrayIterator;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class AnalyzingCompletionTest extends LuceneTestCase {
  
  /** this is basically the WFST test ported to KeywordAnalyzer. so it acts the same */
  public void testKeyword() throws Exception {
    TermFreq keys[] = new TermFreq[] {
        new TermFreq("foo", 50),
        new TermFreq("bar", 10),
        new TermFreq("barbar", 12),
        new TermFreq("barbara", 6)
    };
    
    AnalyzingCompletionLookup suggester = new AnalyzingCompletionLookup(new MockAnalyzer(random(), MockTokenizer.KEYWORD, false));
    suggester.build(new TermFreqArrayIterator(keys));
    
    // top N of 2, but only foo is available
    List<LookupResult> results = suggester.lookup(_TestUtil.stringToCharSequence("f", random()), false, 2);
    assertEquals(1, results.size());
    assertEquals("foo", results.get(0).key.toString());
    assertEquals(50, results.get(0).value, 0.01F);
    
    // top N of 1 for 'bar': we return this even though barbar is higher
    results = suggester.lookup(_TestUtil.stringToCharSequence("bar", random()), false, 1);
    assertEquals(1, results.size());
    assertEquals("bar", results.get(0).key.toString());
    assertEquals(10, results.get(0).value, 0.01F);
    
    // top N Of 2 for 'b'
    results = suggester.lookup(_TestUtil.stringToCharSequence("b", random()), false, 2);
    assertEquals(2, results.size());
    assertEquals("barbar", results.get(0).key.toString());
    assertEquals(12, results.get(0).value, 0.01F);
    assertEquals("bar", results.get(1).key.toString());
    assertEquals(10, results.get(1).value, 0.01F);
    
    // top N of 3 for 'ba'
    results = suggester.lookup(_TestUtil.stringToCharSequence("ba", random()), false, 3);
    assertEquals(3, results.size());
    assertEquals("barbar", results.get(0).key.toString());
    assertEquals(12, results.get(0).value, 0.01F);
    assertEquals("bar", results.get(1).key.toString());
    assertEquals(10, results.get(1).value, 0.01F);
    assertEquals("barbara", results.get(2).key.toString());
    assertEquals(6, results.get(2).value, 0.01F);
  }
  
  // TODO: more tests
  /**
   * basic "standardanalyzer" test with stopword removal
   */
  public void testStandard() throws Exception {
    TermFreq keys[] = new TermFreq[] {
        new TermFreq("the ghost of christmas past", 50),
    };
    
    Analyzer standard = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true, MockTokenFilter.ENGLISH_STOPSET, false);
    AnalyzingCompletionLookup suggester = new AnalyzingCompletionLookup(standard);
    suggester.build(new TermFreqArrayIterator(keys));
    
    List<LookupResult> results = suggester.lookup(_TestUtil.stringToCharSequence("the ghost of chris", random()), false, 1);
    assertEquals(1, results.size());
    assertEquals("the ghost of christmas past", results.get(0).key.toString());
    assertEquals(50, results.get(0).value, 0.01F);

    // omit the 'the' since its a stopword, its suggested anyway
    results = suggester.lookup(_TestUtil.stringToCharSequence("ghost of chris", random()), false, 1);
    assertEquals(1, results.size());
    assertEquals("the ghost of christmas past", results.get(0).key.toString());
    assertEquals(50, results.get(0).value, 0.01F);

    // omit the 'the' and 'of' since they are stopwords, its suggested anyway
    results = suggester.lookup(_TestUtil.stringToCharSequence("ghost chris", random()), false, 1);
    assertEquals(1, results.size());
    assertEquals("the ghost of christmas past", results.get(0).key.toString());
    assertEquals(50, results.get(0).value, 0.01F);
  }
  
  public void testRandom() throws Exception {
    int numWords = atLeast(1000);
    
    final TreeMap<String,Long> slowCompletor = new TreeMap<String,Long>();
    final TreeSet<String> allPrefixes = new TreeSet<String>();
    
    TermFreq[] keys = new TermFreq[numWords];
    
    for (int i = 0; i < numWords; i++) {
      String s;
      while (true) {
        // TODO: would be nice to fix this slowCompletor/comparator to
        // use full range, but we might lose some coverage too...
        s = _TestUtil.randomSimpleString(random());
        if (!slowCompletor.containsKey(s)) {
          break;
        }
      }
      
      for (int j = 1; j < s.length(); j++) {
        allPrefixes.add(s.substring(0, j));
      }
      // we can probably do Integer.MAX_VALUE here, but why worry.
      int weight = random().nextInt(1<<24);
      slowCompletor.put(s, (long)weight);
      keys[i] = new TermFreq(s, weight);
    }

    AnalyzingCompletionLookup suggester = new AnalyzingCompletionLookup(new MockAnalyzer(random(), MockTokenizer.KEYWORD, false), false);
    suggester.build(new TermFreqArrayIterator(keys));
    
    for (String prefix : allPrefixes) {
    
      final int topN = _TestUtil.nextInt(random(), 1, 10);
      List<LookupResult> r = suggester.lookup(_TestUtil.stringToCharSequence(prefix, random()), false, topN);

      // 2. go thru whole treemap (slowCompletor) and check its actually the best suggestion
      final List<LookupResult> matches = new ArrayList<LookupResult>();

      // TODO: could be faster... but its slowCompletor for a reason
      for (Map.Entry<String,Long> e : slowCompletor.entrySet()) {
        if (e.getKey().startsWith(prefix)) {
          matches.add(new LookupResult(e.getKey(), e.getValue().longValue()));
        }
      }

      assertTrue(matches.size() > 0);
      Collections.sort(matches, new Comparator<LookupResult>() {
        public int compare(LookupResult left, LookupResult right) {
          int cmp = Float.compare(right.value, left.value);
          if (cmp == 0) {
            return left.compareTo(right);
          } else {
            return cmp;
          }
        }
      });
      if (matches.size() > topN) {
        matches.subList(topN, matches.size()).clear();
      }

      assertEquals(matches.size(), r.size());

      for(int hit=0;hit<r.size();hit++) {
        //System.out.println("  check hit " + hit);
        assertEquals(matches.get(hit).key.toString(), r.get(hit).key.toString());
        assertEquals(matches.get(hit).value, r.get(hit).value, 0f);
      }
    }
  }
}
