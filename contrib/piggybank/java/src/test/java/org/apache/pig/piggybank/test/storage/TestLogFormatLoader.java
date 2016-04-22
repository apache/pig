 /*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.piggybank.test.storage;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.apache.pig.builtin.mock.Storage.map;
import static org.junit.Assert.assertEquals;

public class TestLogFormatLoader {
    @Test
    public void testLogFormatLoader() throws Exception {
        final String logformat = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\" \"%{Cookie}i\"";
        final String testLine = "2001:980:91c0:1:8d31:a232:25e5:85d - - [05/Sep/2010:11:27:50 +0200] " +
                "\"GET /b/ss/advbolprod2/1/H.22.1/s73176445413647?AQB=1&pccr=true&vidn=27F07A1B85012045-403" +
                "&&ndh=1&t=19%2F5%2F2012%2023%3A51%3A27%202%20-120&ce=UTF-8&ns=bol&pageName=%2Fnl%2Fp%2Ffissler-" +
                "speciaal-pannen-grillpan-28-x-28-cm%2F9200000002876066%2F&g=http%3A%2F%2Fwww.bol.com%2Fnl%2Fp%2F" +
                "fissler-speciaal-pannen-grillpan-28-x-28-cm%2F9200000002876066%2F%3Fpromo%3Dkoken-pannen_303_hs-" +
                "koken-pannen-afj-120601_B3_product_1_9200000002876066%26bltg.pg_nm%3Dkoken-pannen%26bltg.slt_id%3D" +
                "303%26bltg.slt_nm%3Dhs-koken-pannen-afj-120601%26bltg.slt_p&r=http%3A%2F%2Fwww.bol.com%2Fnl%2Fm%2F" +
                "koken-tafelen%2Fkoken-pannen%2FN%2F11766%2Findex.html%3Fblabla%3Dblablawashere&cc=EUR&ch=D%3Dv3&" +
                "server=ps316&events=prodView%2Cevent1%2Cevent2%2Cevent31&products=%3B9200000002876066%3B%3B%3B%3B" +
                "evar3%3Dkth%7Cevar8%3D9200000002876066_Fissler%20Speciaal%20Pannen%20-%20Grillpan%20-%2028%20x%2028" +
                "%20cm%7Cevar35%3D170%7Cevar47%3DKTH%7Cevar9%3DNew%7Cevar40%3Dno%20reviews%2C%3B%3B%3B%3Bevent31%3D423" +
                "&c1=catalog%3Akth%3Aproduct-detail&v1=D%3Dc1&h1=catalog%2Fkth%2Fproduct-detail&h2=D%3DpageName&v3=kth" +
                "&l3=endeca_001-mensen_default%2Cendeca_exact-boeken_default%2Cendeca_verschijningsjaar_default%2C" +
                "endeca_hardgoodscategoriesyn_default%2Cendeca_searchrank-hadoop_default%2Cendeca_genre_default%2C" +
                "endeca_uitvoering_default&v4=ps316&v6=koken-pannen_303_hs-koken-pannen-afj-120601_B3_product_1_" +
                "9200000002876066&v10=Tu%2023%3A30&v12=logged%20in&v13=New&c25=niet%20ssl&c26=3631&" +
                "c30=1.2.3.4.1323208998208762&v31=2000285551&c45=20120619235127&c46=20120501%204.3.4.1&" +
                "c47=D%3Ds_vi&c49=%2Fnl%2Fcatalog%2Fproduct-detail.jsp&c50=%2Fnl%2Fcatalog%2Fproduct-detail.jsp&" +
                "v51=www.bol.com&s=1280x800&c=24&j=1.7&v=N&k=Y&bw=1280&bh=272&p=Shockwave%20Flash%3B&AQE=1 " +
                "HTTP/1.1\" 200 23617 \"http://www.google.nl/imgres?imgurl=http://daniel_en_sander.basjes.nl/" +
                "fotos/geboorte-kaartje/geboortekaartje-binnenkant.jpg&imgrefurl=http://daniel_en_sander.basjes.nl/" +
                "fotos/geboorte-kaartje&usg=__LDxRMkacRs6yLluLcIrwoFsXY6o=&h=521&w=1024&sz=41&hl=nl&start=13&zoom=1" +
                "&um=1&itbs=1&tbnid=Sqml3uGbjoyBYM:&tbnh=76&tbnw=150&prev=/images%3Fq%3Dbinnenkant%2Bgeboortekaartje" +
                "%26um%3D1%26hl%3Dnl%26sa%3DN%26biw%3D1882%26bih%3D1014%26tbs%3Disch:1\" " +
                "\"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; nl-nl) AppleWebKit/533.17.8 (KHTML, like Gecko) " +
                "Version/5.0.1 Safari/533.17.8\" \"jquery-ui-theme=Eggplant; BuI=SomeThing; " +
                "Apache=127.0.0.1.1351111543699529\"";

        PigServer pigServer = new PigServer(ExecType.LOCAL);
        Storage.Data data = resetData(pigServer);

        ArrayList<String[]> input = new ArrayList<String[]>();
        input.add(new String[] { testLine });
        String filename = TestHelper.createTempFile(input, " ");
        filename = filename.replace("\\", "\\\\");

        pigServer.registerQuery(
            "Clicks = " +
            "    LOAD '" + filename + "' " +
            "    USING org.apache.pig.piggybank.storage.apachelog.LogFormatLoader(" +
            "            '"+logformat+"'," +
            "            'IP:connection.client.host'," +
            "            'TIME.STAMP:request.receive.time'," +
            "       '-map:request.firstline.uri.query.g:HTTP.URI'," +
            "            'STRING:request.firstline.uri.query.g.query.promo'," +
            "            'STRING:request.firstline.uri.query.g.query.*'," +
            "            'STRING:request.firstline.uri.query.s'," +
            "       '-map:request.firstline.uri.query.r:HTTP.URI'," +
            "            'STRING:request.firstline.uri.query.r.query.blabla'," +
            "            'HTTP.COOKIE:request.cookies.bui'," +
            "            'HTTP.USERAGENT:request.user-agent'" +
            "            )" +
            "         AS (" +
            "            ConnectionClientHost," +
            "            RequestReceiveTime," +
            "            Promo," +
            "            QueryParams:map[]," +
            "            ScreenResolution," +
            "            GoogleQuery," +
            "            BUI," +
            "            RequestUseragent" +
            "            );"
            );

        pigServer.registerQuery("STORE Clicks INTO 'Clicks' USING mock.Storage();");

        List<Tuple> out = data.get("Clicks");

        assertEquals(1, out.size());

        Tuple actual = out.get(0);
        Tuple expected = tuple(
            "2001:980:91c0:1:8d31:a232:25e5:85d",
            "05/Sep/2010:11:27:50 +0200",
            "koken-pannen_303_hs-koken-pannen-afj-120601_B3_product_1_9200000002876066",
            map(
                "promo"       , "koken-pannen_303_hs-koken-pannen-afj-120601_B3_product_1_9200000002876066",
                "bltg.pg_nm"  , "koken-pannen",
                "bltg.slt_nm" , "hs-koken-pannen-afj-120601",
                "bltg.slt_id" , "303",
                "bltg.slt_p"  , ""
            ),
            "1280x800",
            "blablawashere",
            "SomeThing",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; nl-nl) AppleWebKit/533.17.8 " +
                "(KHTML, like Gecko) Version/5.0.1 Safari/533.17.8"
        );

        assertEquals(expected, actual);
    }

}
