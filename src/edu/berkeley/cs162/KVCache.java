/**
 * Implementation of a set-associative cache.
 *
 * @author Mosharaf Chowdhury (http://www.mosharaf.com)
 * @author Prashanth Mohan (http://www.cs.berkeley.edu/~prmohan)
 * <p>
 * Copyright (c) 2012, University of California at Berkeley
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of University of California, Berkeley nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package edu.berkeley.cs162;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;


/**
 * A set-associate cache which has a fixed maximum number of sets (numSets).
 * Each set has a maximum number of elements (MAX_ELEMS_PER_SET).
 * If a set is full and another entry is added, an entry is dropped based on the eviction policy.
 */
public class KVCache implements KeyValueInterface {
    private int numSets = 100;
    private int maxElemsPerSet = 10;

    private List<CacheSet> cacheSets;

    private class CacheSet {
        private Map<String, SetEntry> map;
        private ReentrantReadWriteLock lock;
        private int keyPtr;

        public Map<String, SetEntry> getMap() {
            return map;
        }

        public WriteLock getWriteLock() {
            return lock.writeLock();
        }

        public ReadLock getReadLock() {
            return lock.readLock();
        }

        public int getKeyPtr() {
            return keyPtr;
        }

        public void put(String key, String value) {
            if (map.size() == maxElemsPerSet) {
                String keys[] = new String[maxElemsPerSet];
                keys = map.keySet().toArray(keys);

                while (true) {
                    if (keyPtr == maxElemsPerSet) {
                        keyPtr = 0;
                    }
                    String curKey = keys[keyPtr];
                    SetEntry curEntry = map.get(curKey);
                    if (!curEntry.referenced) {
                        map.remove(curKey);
                        break;
                    }
                    keyPtr++;
                }
            }
            map.put(key, new SetEntry(value));
        }

        public String get(String key) {
            if (map.containsKey(key)) {
                SetEntry result = map.get(key);
                result.referenced = true;
                return result.value;
            }
            return null;
        }

        public void del(String key) {
            if (map.containsKey(key)) {
                map.remove(key);
            }
        }

        public CacheSet(Map<String, SetEntry> map) {
            this.map = map;
            final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
            this.lock = lock;
            this.keyPtr = 0;
        }
    }

    private class SetEntry {
        boolean referenced;
        String value;

        public SetEntry(String value) {
            this.referenced = false;
            this.value = value;
        }

        public SetEntry() {
            this(null);
        }
    }

    /**
     * Creates a new LRU cache.
     * @param maxElemsPerSet the maximum number of entries that will be kept in this cache.
     */
    public KVCache(int numSets, int maxElemsPerSet) {
        this.numSets = numSets;
        this.maxElemsPerSet = maxElemsPerSet;
        this.cacheSets = new ArrayList<>();

        for (int i = 0; i < numSets; i++) {
            cacheSets.add(new CacheSet(new LinkedHashMap<>()));
        }
    }

    /**
     * Retrieves an entry from the cache.
     * Assumes the corresponding set has already been locked for writing.
     * @param key the key whose associated value is to be returned.
     * @return the value associated to this key, or null if no value with this key exists in the cache.
     */
    public String get(String key) {
        // Must be called before anything else
        AutoGrader.agCacheGetStarted(key);
        AutoGrader.agCacheGetDelay();

        int setId = getSetId(key);
        CacheSet set = cacheSets.get(setId);
        String result = set.get(key);

        // Must be called before returning
        AutoGrader.agCacheGetFinished(key);
        return result;
    }

    /**
     * Adds an entry to this cache.
     * If an entry with the specified key already exists in the cache, it is replaced by the new entry.
     * If the cache is full, an entry is removed from the cache based on the eviction policy
     * Assumes the corresponding set has already been locked for writing.
     * @param key    the key with which the specified value is to be associated.
     * @param value    a value to be associated with the specified key.
     * @return true is something has been overwritten
     */
    public void put(String key, String value) {
        // Must be called before anything else
        AutoGrader.agCachePutStarted(key, value);
        AutoGrader.agCachePutDelay();

        int setId = getSetId(key);
        CacheSet set = cacheSets.get(setId);
        set.put(key, value);

        // Must be called before returning
        AutoGrader.agCachePutFinished(key, value);
    }

    /**
     * Removes an entry from this cache.
     * Assumes the corresponding set has already been locked for writing.
     * @param key    the key with which the specified value is to be associated.
     */
    public void del(String key) {
        // Must be called before anything else
        AutoGrader.agCacheGetStarted(key);
        AutoGrader.agCacheDelDelay();

        int setId = getSetId(key);
        CacheSet set = cacheSets.get(setId);
        set.del(key);

        // Must be called before returning
        AutoGrader.agCacheDelFinished(key);
    }

    /**
     * @param key
     * @return the write lock of the set that contains key.
     */
    public WriteLock getWriteLock(String key) {
        return cacheSets.get(getSetId(key)).getWriteLock();
    }

    public ReadLock getReadLock(String key) {
        return cacheSets.get(getSetId(key)).getReadLock();
    }

    /**
     *
     * @param key
     * @return set of the key
     */
    private int getSetId(String key) {
        return Math.abs(key.hashCode()) % numSets;
    }

    public String toXML() {
        try {
            Element root, setEle, entryEle, keyEle, valueEle;

            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.newDocument();
            doc.setXmlStandalone(true);

            root = doc.createElement("KVCache");
            doc.appendChild(root);

            for (int i = 0; i < this.numSets; i++) {
                CacheSet set = cacheSets.get(i);
                String keys[] = new String[maxElemsPerSet];
                keys = set.getMap().keySet().toArray(keys);

                setEle = doc.createElement("Set");
                setEle.setAttribute("Id", Integer.toString(i));

                for (int j = 0; j < this.maxElemsPerSet; j++) {
                    entryEle = doc.createElement("CacheEntry");
                    keyEle = doc.createElement("Key");
                    valueEle = doc.createElement("Value");

                    if (j >= keys.length) {
                        keyEle.appendChild(doc.createTextNode(""));
                        valueEle.appendChild(doc.createTextNode(""));
                        entryEle.setAttribute("isReferenced", Boolean.toString(false));
                        entryEle.setAttribute("isValid", Boolean.toString(false));
                    } else {
                        String key = keys[j];
                        SetEntry entry = set.getMap().get(key);
                        keyEle.appendChild(doc.createTextNode(key));
                        valueEle.appendChild(doc.createTextNode(entry.value));
                        entryEle.setAttribute("isReferenced", Boolean.toString(entry.referenced));
                        entryEle.setAttribute("isValid", Boolean.toString(true));
                    }
                    entryEle.appendChild(keyEle);
                    entryEle.appendChild(valueEle);
                    setEle.appendChild(entryEle);
                }
                root.appendChild(setEle);
            }

            StringWriter stringWriter = new StringWriter();
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.transform(new DOMSource(doc), new StreamResult(stringWriter));
            return stringWriter.toString();
        } catch (ParserConfigurationException | TransformerException e) {
            e.printStackTrace();
            return null;
        }
    }
}
