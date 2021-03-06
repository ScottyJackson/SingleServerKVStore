/**
 * Sample instantiation of the Key-Value client
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

import java.io.IOException;

import edu.berkeley.cs162.KVClient;

public class Client {
    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        KVClient kc = new KVClient("localhost", 8080);
        try {
            String apple = "apple";
            String aardvark = "aardvark";
            String orange = "orange";
            String value;

            System.out.println("putting (a, apple)");
            kc.put("a", apple);
            System.out.println("ok");

            System.out.println();

            System.out.println("putting (o, orange)");
            kc.put("o", orange);
            System.out.println("ok");

            System.out.println();

            System.out.println("getting key = a");
            value = kc.get("a");
            System.out.println("returned value: " + value);

            System.out.println();

            System.out.println("putting (a, aardvark)");
            kc.put("a", aardvark);
            System.out.println("ok");

            System.out.println("getting key = a");
            value = kc.get("a");
            System.out.println("returned value: " + value);

            System.out.println();

            System.out.println("deleting key = o");
            kc.del("o");
            System.out.println("ok");

            System.out.println();

            System.out.println("getting key = o");
            value = kc.get("o");
            System.out.println("returned value: " + value);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
