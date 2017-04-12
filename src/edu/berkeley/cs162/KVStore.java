/**
 * Persistent Key-Value storage layer. Current implementation is transient,
 * but assume to be backed on disk when you do your project.
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

import org.w3c.dom.*;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;


/**
 * This is a dummy KeyValue Store. Ideally this would go to disk,
 * or some other backing store. For this project, we simulate the disk like
 * system using a manual delay.
 *
 *
 *
 */
public class KVStore implements KeyValueInterface {
    private Map<String, String> store = null;
    private ReentrantReadWriteLock lock;

    public KVStore() {
        resetStore();
        lock = new ReentrantReadWriteLock();
    }

    private void resetStore() {
        store = new HashMap<String, String>();
    }

    public void put(String key, String value) throws KVException {
        AutoGrader.agStorePutStarted(key, value);

        try {
            putDelay();
            store.put(key, value);
        } finally {
            AutoGrader.agStorePutFinished(key, value);
        }
    }

    public String get(String key) throws KVException {
        AutoGrader.agStoreGetStarted(key);

        try {
            getDelay();
            String retVal = store.get(key);
            if (retVal == null) {
                KVMessage msg = new KVMessage("resp", "key \"" + key + "\" does not exist in store");
                throw new KVException(msg);
            }
            return retVal;
        } finally {
            AutoGrader.agStoreGetFinished(key);
        }
    }

    public void del(String key) throws KVException {
        AutoGrader.agStoreDelStarted(key);

        try {
            delDelay();
            if (store.containsKey(key)) {
                store.remove(key);
            } else {
                throw new KVException(new KVMessage("resp", "key \"" + key + "\" does not exist in store"));
            }
        } finally {
            AutoGrader.agStoreDelFinished(key);
        }
    }

    public ReentrantReadWriteLock getLock() {
        return lock;
    }

    private void getDelay() {
        AutoGrader.agStoreDelay();
    }

    private void putDelay() {
        AutoGrader.agStoreDelay();
    }

    private void delDelay() {
        AutoGrader.agStoreDelay();
    }

    public String toXML() {
        try {
            Element root, pairEle, keyEle, valueEle;

            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();

            Document doc = db.newDocument();
            doc.setXmlStandalone(true);

            root = doc.createElement("KVStore");
            doc.appendChild(root);

            for (Map.Entry<String, String> entry : store.entrySet()) {
                pairEle = doc.createElement("KVPair");

                keyEle = doc.createElement("Key");
                keyEle.appendChild(doc.createTextNode(entry.getKey()));
                valueEle = doc.createElement("Value");
                valueEle.appendChild(doc.createTextNode(entry.getValue()));

                pairEle.appendChild(keyEle);
                pairEle.appendChild(valueEle);

                root.appendChild(pairEle);
            }

            StringWriter stringWriter = new StringWriter();
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.transform(new DOMSource(doc), new StreamResult(stringWriter));
            return stringWriter.toString();

        } catch (ParserConfigurationException pce) {
            pce.printStackTrace();
            return null;
        } catch (TransformerException tfe) {
            tfe.printStackTrace();
            return null;
        }
    }

    public void dumpToFile(String fileName) {
        byte data[] = this.toXML().getBytes();
        Path file = Paths.get(fileName);
        try {
            Files.write(file, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Replaces the contents of the store with the contents of a file
     * written by dumpToFile; the previous contents of the store are lost.
     * @param fileName the file to be read.
     */
    public void restoreFromFile(String fileName) {
        try {
            byte data[] = Files.readAllBytes(Paths.get(fileName));
            String storeFile = new String(data);
            resetStore();

            Node pairNode, keyNode, valueNode;
            NodeList pairEles, children;

            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            InputSource source = new InputSource(new StringReader(storeFile));

            Document doc = dbf.newDocumentBuilder().parse(source);
            doc.getDocumentElement().normalize();

            pairEles = doc.getElementsByTagName("KVPair");

            for (int i = 0; i < pairEles.getLength(); i++) {
                pairNode = pairEles.item(i);
                children = pairNode.getChildNodes();

                keyNode = children.item(0);
                valueNode = children.item(1);
                String key = keyNode.getTextContent();
                String value = valueNode.getTextContent();

                store.put(key, value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
