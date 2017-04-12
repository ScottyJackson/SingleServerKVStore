/**
 * Slave Server component of a KeyValue store
 *
 * @author Mosharaf Chowdhury (http://www.mosharaf.com)
 * @author Prashanth Mohan (http://www.cs.berkeley.edu/~prmohan)
 *
 * Copyright (c) 2012, University of California at Berkeley
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *  * Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of University of California, Berkeley nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED. IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY
 *  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package edu.berkeley.cs162;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

/**
 * This class defines the slave key value servers. Each individual KVServer
 * would be a fully functioning Key-Value server. For Project 3, you would
 * implement this class. For Project 4, you will have a Master Key-Value server
 * and multiple of these slave Key-Value servers, each of them catering to a
 * different part of the key namespace.
 *
 */
public class KVServer implements KeyValueInterface {
	private final KVStore dataStore;
	private KVCache dataCache = null;

	private static final int MAX_KEY_SIZE = 256;
	private static final int MAX_VAL_SIZE = 256 * 1024;
	private static final String OVERSIZED_KEY = "Key Error: Oversized Key";
	private static final String UNDERSIZED_KEY = "Key Error: Undersized Key";
	private static final String OVERSIZED_VALUE = "Value Error: Oversized Value";
	private static final String UNDERSIZED_VALUE = "Value Error: Undersized Value";
	private static final String MSG_FORMAT = "Message Format Incorrect";

	/**
	 * @param numSets number of sets in the data Cache.
	 */
	public KVServer(int numSets, int maxElemsPerSet) {
		dataStore = new KVStore();
		dataCache = new KVCache(numSets, maxElemsPerSet);

		AutoGrader.registerKVServer(dataStore, dataCache);
	}

	public void put(String key, String value) throws KVException {
		// Must be called before anything else
		AutoGrader.agKVServerPutStarted(key, value);

		checkKeySize(key);
		checkValueSize(value);

        WriteLock cacheWrLock = dataCache.getWriteLock(key);
        WriteLock storeWrLock = dataStore.getLock().writeLock();
        System.out.println("Locking StoreWriteLock");
        storeWrLock.lock();
        System.out.println("Locking Cache");
        cacheWrLock.lock();
        try {
            System.out.printf("putting (%s -> %s)\n", key, value);
            dataStore.put(key, value);
            dataCache.put(key, value);
        } finally {
            // Must be called before return or abnormal exit
            AutoGrader.agKVServerPutFinished(key, value);
            System.out.println("Unlocking Cache");
            cacheWrLock.unlock();
            System.out.println("Unlocking StoreWriteLock");
            storeWrLock.unlock();
        }
	}

	public String get (String key) throws KVException {
		// Must be called before anything else
		AutoGrader.agKVServerGetStarted(key);

		checkKeySize(key);

        String result;

        WriteLock cacheWrLock = dataCache.getWriteLock(key);
        System.out.println("Locking Cache");
        cacheWrLock.lock();
        try {
            result = dataCache.get(key);
            if (result == null) {
                System.out.println("Cache Miss: Looking in Data Store");

                ReadLock storeRLock = dataStore.getLock().readLock();
                System.out.println("Locking StoreReadLock");
                storeRLock.lock();
                try {
                    result = getFromStore(key);
                } finally {
                    System.out.println("Unlocking StoreReadLock");
                    storeRLock.unlock();
                }
                System.out.printf("putting (%s -> %s) in cache", key, result);
                dataCache.put(key, result);
            } else {
                System.out.println("Cache Hit!");
            }
        } finally {
            // Must be called before return or abnormal exit
            AutoGrader.agKVServerGetFinished(key);
            System.out.println("Unlocking Cache");
            cacheWrLock.unlock();
        }
		return result;
	}

	public void del (String key) throws KVException {
		// Must be called before anything else
		AutoGrader.agKVServerDelStarted(key);

        checkKeySize(key);

        WriteLock cacheWrLock = dataCache.getWriteLock(key);
        WriteLock storeWrLock = dataStore.getLock().writeLock();
        System.out.println("Locking StoreWriteLock");
        storeWrLock.lock();
        System.out.println("Locking Cache");
        cacheWrLock.lock();
        try {
            getFromStore(key);

            dataCache.del(key);
            dataStore.del(key);
        } finally {
            // Must be called before return or abnormal exit
            AutoGrader.agKVServerDelFinished(key);
            System.out.println("Unlocking Cache");
            cacheWrLock.unlock();
            System.out.println("Unlocking StoreWriteLock");
            storeWrLock.unlock();
        }
	}

	private String getFromStore(String key) throws KVException {
        String value;
        value = dataStore.get(key);
        if (value == null) {
            throw new KVException(new KVMessage("resp", "Key Does Not Exist"));
        }

        return value;
    }

	private void checkKeySize(String key) throws KVException {
		if (key == null) {
			throw new KVException(new KVMessage("resp", MSG_FORMAT));
		}
		if (key.length() == 0) {
			throw new KVException(new KVMessage("resp", UNDERSIZED_KEY));
		} else if (key.length() > MAX_KEY_SIZE) {
			throw new KVException(new KVMessage("resp", OVERSIZED_KEY));
		}
	}

	private void checkValueSize(String value) throws KVException {
		if (value == null) {
			throw new KVException(new KVMessage("resp", MSG_FORMAT));
		}
		if (value.length() == 0) {
			throw new KVException(new KVMessage("resp", UNDERSIZED_VALUE));
		} else if (value.length() > MAX_VAL_SIZE) {
			throw new KVException(new KVMessage("resp", OVERSIZED_VALUE));
		}
	}

	private void unlock(WriteLock lock) {
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
        }
    }
}
