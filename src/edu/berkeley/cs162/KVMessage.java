/**
 * XML Parsing library for the key-value store
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

import java.io.*;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.*;
import org.xml.sax.*;

/**
 * This is the object that is used to generate messages the XML based messages 
 * for communication between clients and servers. 
 */
public class KVMessage {
    private String msgType = null;
    private String key = null;
    private String value = null;
    private String message = null;

    private static final String[] MSG_TYPES = {"getreq", "putreq", "delreq", "resp"};
    private static final Set<String> TYPE_SET = new HashSet<>(Arrays.asList(MSG_TYPES));

    private static final String MSG_FORMAT_ERR = "Message Format Incorrect";
    private static final String XML_ERR = "XML Error: Received unparseable message";
    private static final String NETWORK_ERR = "Network Error: Could not receive data";

    public final String getKey() {
        return key;
    }

    public final void setKey(String key) {
        this.key = key;
    }

    public final String getValue() {
        return value;
    }

    public final void setValue(String value) {
        this.value = value;
    }

    public final String getMessage() {
        return message;
    }

    public final void setMessage(String message) {
        this.message = message;
    }

    public String getMsgType() {
        return msgType;
    }

    /* Solution from http://weblogs.java.net/blog/kohsuke/archive/2005/07/socket_xml_pitf.html */
    private class NoCloseInputStream extends FilterInputStream {
        public NoCloseInputStream(InputStream in) {
            super(in);
        }

        public void close() {
        } // ignore close
    }

    /***
     *
     * @param msgType
     * @throws KVException of type "resp" with message "Message format incorrect" if msgType is unknown
     */
    public KVMessage(String msgType) throws KVException {
        if (!TYPE_SET.contains(msgType)) {
            throw new KVException(new KVMessage("resp", MSG_FORMAT_ERR));
        }

        this.msgType = msgType;
    }

    public KVMessage(String msgType, String message) throws KVException {
        if (!TYPE_SET.contains(msgType)) {
            throw new KVException(new KVMessage("resp", MSG_FORMAT_ERR));
        }

        this.msgType = msgType;
        this.message = message;
    }

    /***
     * Parse KVMessage from socket's input stream
     * @param sock Socket to receive from
     * @throws KVException if there is an error in parsing the message. The exception should be of type "resp and message should be :
     * a. "XML Error: Received unparseable message" - if the received message is not valid XML.
     * b. "Network Error: Could not receive data" - if there is a network error causing an incomplete parsing of the message.
     * c. "Message format incorrect" - if there message does not conform to the required specifications. Examples include incorrect message type. 
     */
    public KVMessage(Socket sock) throws KVException {
        ObjectInputStream in = null;
        String type, kvMsg;
        Node rootNode;
        NodeList rootEle, keyEle, valueEle, msgEle;

        try {
            in = new ObjectInputStream(new NoCloseInputStream(sock.getInputStream()));
            kvMsg = (String)in.readObject();

            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            InputSource source = new InputSource(new StringReader(kvMsg));
            Document doc = dbf.newDocumentBuilder().parse(source);
            doc.getDocumentElement().normalize();

            rootEle = doc.getElementsByTagName("KVMessage");
            if (rootEle.getLength() != 1) {
                throw new KVException(new KVMessage("resp", MSG_FORMAT_ERR));
            }

            rootNode = rootEle.item(0);
            type = parseAttribute(rootNode, "type", 0);
            if (!TYPE_SET.contains(type)) {
                throw new KVException(new KVMessage("resp", MSG_FORMAT_ERR));
            }
            this.msgType = type;

            if (this.msgType.equals("putreq") || this.msgType.equals("getreq") || this.msgType.equals("delreq")) {
                this.key = parseElement(doc, "Key");

                if (msgType.equals("putreq")) {
                    this.value = parseElement(doc, "Value");
                }
            } else {
                keyEle = doc.getElementsByTagName("Key");
                valueEle = doc.getElementsByTagName("Value");
                msgEle = doc.getElementsByTagName("Message");

                if (keyEle.getLength() == 1 && valueEle.getLength() == 1 && msgEle.getLength() == 0) {
                    this.key = parseElement(doc, "Key");
                    this.value = parseElement(doc, "Value");
                } else if (msgEle.getLength() == 1 && keyEle.getLength() == 0 && valueEle.getLength() == 0) {
                    this.message = parseElement(doc, "Message");
                } else {
                    throw new KVException(new KVMessage("resp", MSG_FORMAT_ERR));
                }
            }
        } catch (IOException e) {
            throw new KVException(new KVMessage("resp", NETWORK_ERR));
        } catch (ClassNotFoundException|ParserConfigurationException e) {
            e.printStackTrace();
            throw new KVException(new KVMessage("resp", "Unknown Error: " + e.getMessage()));
        } catch (SAXException e) {
            throw new KVException(new KVMessage("resp", XML_ERR));
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new KVException(new KVMessage("resp", "Unknown Error: " + e.getMessage()));
            }
        }
    }

    private String parseAttribute(Node node, String attrName, int idx) throws KVException {
        NamedNodeMap attributes = node.getAttributes();
        if (!attributes.item(idx).getNodeName().equals(attrName)) {
            throw new KVException(new KVMessage("resp", MSG_FORMAT_ERR));
        }
        return attributes.item(0).getNodeValue();
    }

    private String parseElement(Document doc, String tagName) throws KVException {
        NodeList ele = doc.getElementsByTagName(tagName);
        if (ele.getLength() != 1) {
            throw new KVException(new KVMessage("resp", MSG_FORMAT_ERR));
        }

        Node node = ele.item(0);
        return node.getTextContent();
    }

    /**
     * Generate the XML representation for this message.
     * @return the XML String
     * @throws KVException if not enough data is available to generate a valid KV XML message
     */
    public String toXML() throws KVException {
        try {
            Element root, keyEle, valueEle, msgEle;

            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();

            Document doc = db.newDocument();
            doc.setXmlStandalone(true);

            root = doc.createElement("KVMessage");
            root.setAttribute("type", this.msgType);

            doc.appendChild(root);

            if (this.msgType.equals("getreq") || this.msgType.equals("putreq") || this.msgType.equals("delreq")) {
                if (this.key == null) {
                    throw new KVException(new KVMessage("resp", "XML Error: No Key"));
                }

                createAndAppend(doc, "Key", this.key, root);

                if (this.msgType.equals("putreq")) {
                    if (this.value == null) {
                        throw new KVException(new KVMessage("resp", "XML Error: No Value"));
                    }

                    createAndAppend(doc, "Value", this.value, root);
                }
            } else {
                if (this.key != null && this.value != null && this.message == null) {
                    createAndAppend(doc, "Key", this.key, root);
                    createAndAppend(doc, "Value", this.value, root);
                } else if (this.message != null && this.key == null && this.value == null) {
                    createAndAppend(doc, "Message", this.message, root);
                } else {
                    throw new KVException(new KVMessage("resp", "XML Error: not enough data"));
                }
            }

            StringWriter stringWriter = new StringWriter();
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.transform(new DOMSource(doc), new StreamResult(stringWriter));
            return stringWriter.toString();
        } catch (ParserConfigurationException|TransformerException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void createAndAppend(Document doc, String tagName, String eleValue, Element parent) {
        Element ele = doc.createElement(tagName);
        ele.appendChild(doc.createTextNode(eleValue));
        parent.appendChild(ele);
    }

    public void sendMessage(Socket sock) throws KVException {
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(sock.getOutputStream());
            String kvMsg = this.toXML();
            out.writeObject(kvMsg);
            out.flush();
        } catch (KVException e) {
            try {
                out.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            throw e;
        } catch (IOException e) {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            throw new KVException(new KVMessage("resp", NETWORK_ERR));
        }
    }
}


