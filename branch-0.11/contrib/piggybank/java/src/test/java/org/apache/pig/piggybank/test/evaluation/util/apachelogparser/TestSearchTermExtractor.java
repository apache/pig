/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.pig.piggybank.test.evaluation.util.apachelogparser;

import java.util.HashMap;

import junit.framework.TestCase;

import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.piggybank.evaluation.util.apachelogparser.SearchEngineExtractor;
import org.apache.pig.piggybank.evaluation.util.apachelogparser.SearchTermExtractor;
import org.junit.Test;

public class TestSearchTermExtractor extends TestCase {
    private static HashMap<String, String> tests = new HashMap<String, String>();
    static {
        tests.put("http://www.google.com/search", null);
        tests.put("http://www.google.com/search?hl=en&q=a+simple+test&btnG=Google+Search", "a simple test");
        tests.put("http://www.google.co.uk/search?hl=en&q=a+simple+test&btnG=Google+Search&meta=", "a simple test");
        tests.put("http://www.google.co.jp/search?hl=ja&q=a+simple+test&btnG=Google+%E6%A4%9C%E7%B4%A2&lr=", "a simple test");
        tests.put("http://search.msn.co.uk/results.aspx?q=a+simple+test&geovar=56&FORM=REDIR", "a simple test");
        tests.put("http://search.msn.com/results.aspx?q=a+simple+test&geovar=56&FORM=REDIR", "a simple test");
        tests.put("http://www.altavista.com/web/results?itag=ody&q=a+simple+test&kgs=1&kls=0", "a simple test");
        tests.put("http://uk.altavista.com/web/results?itag=ody&q=a+simple+test&kgs=1&kls=0", "a simple test");
        tests.put("http://www.blueyonder.co.uk/blueyonder/searches/search.jsp?q=a+simple+test&cr=&sitesearch=&x=0&y=0", "a simple test");
        tests.put("http://www.alltheweb.com/search?cat=web&cs=iso88591&q=a+simple+test&rys=0&itag=crv&_sb_lang=pref", "a simple test");
        tests.put("http://search.lycos.com/?query=a+simple+test&x=0&y=0", "a simple test");
        tests.put("http://search.lycos.co.uk/cgi-bin/pursuit?query=a+simple+test&enc=utf-8&cat=slim_loc&sc=blue", "a simple test");
        tests.put("http://www.hotbot.com/index.php?query=a+simple+test&ps=&loc=searchbox&tab=web&mode=search&currProv=msn", "a simple test");
        tests.put("http://search.yahoo.com/search?p=a+simple+test&fr=FP-tab-web-t400&toggle=1&cop=&ei=UTF-8", "a simple test");
        tests.put("http://uk.search.yahoo.com/search?p=a+simple+test&fr=FP-tab-web-t340&ei=UTF-8&meta=vc%3D", "a simple test");
        tests.put("http://uk.ask.com/web?q=a+simple+test&qsrc=0&o=0&l=dir&dm=all", "a simple test");
        tests.put("http://www.mirago.co.uk/scripts/qhandler.aspx?qry=a+simple+test&x=0&y=0", "a simple test");
        tests.put("http://www.netscape.com/search/?s=a+simple+test", "a simple test");
        tests.put("http://search.aol.co.uk/web?invocationType=ns_uk&query=a%20simple%20test", "a simple test");
        tests.put("http://www.tiscali.co.uk/search/results.php?section=&from=&query=a+simple+test", "a simple test");
        tests.put("http://www.mamma.com/Mamma?utfout=1&qtype=0&query=a+simple+test&Submit=%C2%A0%C2%A0Search%C2%A0%C2%A0", "a simple test");
        tests.put("http://blogs.icerocket.com/search?q=a+simple+test", "a simple test");
        tests.put("http://blogsearch.google.com/blogsearch?hl=en&ie=UTF-8&q=a+simple+test&btnG=Search+Blogs", "a simple test");
        tests.put("http://suche.fireball.de/cgi-bin/pursuit?query=a+simple+test&x=0&y=0&cat=fb_loc&enc=utf-8", "a simple test");
        tests.put("http://suche.web.de/search/web/?allparams=&smode=&su=a+simple+test&webRb=de", "a simple test");
        tests.put("http://www.technorati.com/search/a%20simple%20test", "a simple test");
        tests.put("http://www.feedster.com/search/a%20simple%20test", "a simple test");
        tests.put("http://www.tesco.net/google/searchresults.asp?q=a+simple+test&cr=", "a simple test");
        tests
            .put(
                "http://gps.virgin.net/search/sitesearch?submit.x=1&start=0&format=1&num=10&restrict=site&sitefilter=site%2Fsite_filter.hts&siteresults=site%2Fsite_results.hts&sitescorethreshold=28&q=a+simple+test&scope=UK&x=0&y=0",
                "a simple test");
        tests.put("http://search.bbc.co.uk/cgi-bin/search/results.pl?tab=web&go=homepage&q=a+simple+test&Search.x=0&Search.y=0&Search=Search&scope=all",
            "a simple test");
        tests.put("http://search.live.com/results.aspx?q=a+simple+test&mkt=en-us&FORM=LVSP&go.x=0&go.y=0&go=Search", "a simple test");
        tests.put("http://search.mywebsearch.com/mywebsearch/AJmain.jhtml?searchfor=a+simple+test", "a simple test");
        tests.put("http://www.megasearching.net/m/search.aspx?s=a+simple+test&mkt=&orig=1", "a simple test");
        tests.put("http://www.blueyonder.co.uk/blueyonder/searches/search.jsp?q=a+simple+test&cr=&sitesearch=&x=0&y=0", "a simple test");
        tests.put("http://search.ntlworld.com/ntlworld/search.php?q=a+simple+test&cr=&x=0&y=0", "a simple test");
        tests.put("http://search.orange.co.uk/all?p=_searchbox&pt=resultgo&brand=ouk&tab=web&q=a+simple+test", "a simple test");
        tests.put("http://search.virginmedia.com/results/index.php?channel=other&q=a+simple+test&cr=&x=0&y=0", "a simple test");
        tests.put("http://as.starware.com/dp/search?src_id=305&product=unknown&qry=a+simple+test&z=Find+It", "a simple test");
        tests.put("http://aolsearch.aol.com/aol/search?invocationType=topsearchbox.webhome&query=a+simple+test", "a simple test");
        tests.put("http://www.ask.com/web?q=a+simple+test&qsrc=0&o=0&l=dir", "a simple test");
        tests.put("http://buscador.terra.es/Default.aspx?source=Search&ca=s&query=a%20simple%20test", "a simple test");
        tests.put("http://busca.orange.es/search?origen=home&destino=web&buscar=a+simple+test", "a simple test");
        tests.put("http://search.sweetim.com/search.asp?ln=en&q=a%20simple%20test", "a simple test");
        tests.put("http://search.conduit.com/Results.aspx?q=a+simple+test&hl=en&SelfSearch=1&SearchSourceOrigin=1&ctid=WEBSITE", "a simple test");
        tests.put("http://buscar.ozu.es/index.php?etq=web&q=a+simple+test", "a simple test");
        tests.put("http://buscador.lycos.es/cgi-bin/pursuit?query=a+simple+test&websearchCat=loc&cat=loc&SITE=de&enc=utf-8&ref=sboxlink", "a simple test");
        tests.put("http://search.icq.com/search/results.php?q=a+simple+test&ch_id=st&search_mode=web", "a simple test");
        tests.put("http://search.yahoo.co.jp/search?ei=UTF-8&fr=sfp_as&p=a+simple+test&meta=vc%3D", "a simple test");
        tests.put("http://www.soso.com/q?pid=s.idx&w=a+simple+test", "a simple test");
        tests.put("http://search.myway.com/search/AJmain.jhtml?searchfor=a+simple+test", "a simple test");
        tests.put("http://www.ilmotore.com/newsearch/?query=a+simple+test&where=web", "a simple test");
        tests.put("http://www.ithaki.net/ricerca.cgi?where=italia&query=a+simple+test", "a simple test");
        tests.put("http://ricerca.alice.it/ricerca?f=hpn&qs=a+simple+test", "a simple test");
        tests.put("http://it.search.yahoo.com/search?p=a+simple+test&fr=yfp-t-501&ei=UTF-8&rd=r1", "a simple test");
        tests.put("http://www.excite.it/search/web/results?l=&q=a+simple+test", "a simple test");
        tests.put("http://it.altavista.com/web/results?itag=ody&q=a+simple+test&kgs=1&kls=0", "a simple test");
        tests.put("http://cerca.lycos.it/cgi-bin/pursuit?query=a+simple+test&cat=web", "a simple test");
        tests.put("http://arianna.libero.it/search/abin/integrata.cgi?query=a+simple+test&regione=8&x=0&y=0", "a simple test");
        tests.put("http://www.thespider.it/dir/index.php?q=a+simple+test&search-btn.x=0&search-btn.y=0", "a simple test");
        tests.put("http://godado.it/engine.php?l=it&key=a+simple+test&x=0&y=0", "a simple test");
        tests.put("http://www.simpatico.ws/cgi-bin/links/search.cgi?query=a+simple+test&Vai=Go", "a simple test");
        tests
            .put(
                "http://www.categorico.it/ricerca.html?domains=Categorico.it&q=a+simple+test&sa=Cerca+con+Google&sitesearch=&client=pub-0499722654836507&forid=1&channel=7983145815&ie=ISO-8859-1&oe=ISO-8859-1&cof=GALT%3A%23008000%3BGL%3A1%3BDIV%3A%23336699%3BVLC%3A663399%3BAH%3Acenter%3BBGC%3AFFFFFF%3BLBGC%3A336699%3BALC%3A0000FF%3BLC%3A0000FF%3BT%3A000000%3BGFNT%3A0000FF%3BGIMP%3A0000FF%3BFORID%3A11&hl=it",
                "a simple test");
        tests.put("http://www.cuil.com/search?q=a+simple+test", "a simple test");
        tests.put("http://www.google.com/search?hl=en&lr=&q=a+more%21+complex_+search%24&btnG=Search", "a more! complex_ search$");
        tests.put("http://www.google.co.uk/search?hl=en&q=a+more%21+complex_+search%24&btnG=Google+Search&meta=", "a more! complex_ search$");
        tests.put("http://www.google.co.jp/search?hl=ja&q=a+more%21+complex_+search%24&btnG=Google+%E6%A4%9C%E7%B4%A2&lr=", "a more! complex_ search$");
        tests.put("http://search.msn.com/results.aspx?q=a+more%21+complex_+search%24&FORM=QBHP", "a more! complex_ search$");
        tests.put("http://search.msn.co.uk/results.aspx?q=a+more%21+complex_+search%24&FORM=MSNH&srch_type=0&cp=65001", "a more! complex_ search$");
        tests.put("http://www.altavista.com/web/results?itag=ody&q=a+more%21+complex_+search%24&kgs=1&kls=0", "a more! complex_ search$");
        tests.put("http://uk.altavista.com/web/results?itag=ody&q=a+more%21+complex_+search%24&kgs=1&kls=0", "a more! complex_ search$");
        tests.put("http://www.blueyonder.co.uk/blueyonder/searches/search.jsp?q=a+more%21+complex_+search%24&cr=&sitesearch=&x=0&y=0",
            "a more! complex_ search$");
        tests
            .put("http://www.alltheweb.com/search?cat=web&cs=iso88591&q=a+more%21+complex_+search%24&rys=0&itag=crv&_sb_lang=pref", "a more! complex_ search$");
        tests.put("http://search.lycos.com/?query=a+more%21+complex_+search%24&x=0&y=0", "a more! complex_ search$");
        tests.put("http://search.lycos.co.uk/cgi-bin/pursuit?query=a+more%21+complex_+search%24&enc=utf-8&cat=slim_loc&sc=blue", "a more! complex_ search$");
        tests.put("http://www.hotbot.com/index.php?query=a+more%21+complex_+search%24&ps=&loc=searchbox&tab=web&mode=search&currProv=msn",
            "a more! complex_ search$");
        tests.put("http://search.yahoo.com/search?p=a+more%21+complex_+search%24&fr=FP-tab-web-t400&toggle=1&cop=&ei=UTF-8", "a more! complex_ search$");
        tests.put("http://uk.search.yahoo.com/search?p=a+more%21+complex_+search%24&fr=FP-tab-web-t340&ei=UTF-8&meta=vc%3D", "a more! complex_ search$");
        tests.put("http://uk.ask.com/web?q=a+more%21+complex_+search%24&qsrc=0&o=0&l=dir&dm=all", "a more! complex_ search$");
        tests.put("http://www.mirago.co.uk/scripts/qhandler.aspx?qry=a+more%21+complex_+search%24&x=0&y=0", "a more! complex_ search$");
        tests.put("http://www.netscape.com/search/?s=a+more%21+complex_+search%24", "a more! complex_ search$");
        tests.put("http://search.aol.co.uk/web?query=a+more%21+complex_+search%24&x=0&y=0&isinit=true&restrict=wholeweb", "a more! complex_ search$");
        tests.put("http://www.tiscali.co.uk/search/results.php?section=&from=&query=a+more%21+complex_+search%24", "a more! complex_ search$");
        tests.put("http://www.mamma.com/Mamma?utfout=1&qtype=0&query=a+more%21+complex_+search%24&Submit=%C2%A0%C2%A0Search%C2%A0%C2%A0",
            "a more! complex_ search$");
        tests.put("dud", null);
    }

    @Test
    public void testInstantiation() {
        assertNotNull(new SearchEngineExtractor());
    }

    @Test
    public void testTests() throws Exception {
        SearchTermExtractor searchTermExtractor = new SearchTermExtractor();
        int testCount = 0;
        Tuple input=DefaultTupleFactory.getInstance().newTuple(1);
        for (String key : tests.keySet()) {
            String expected = tests.get(key);

            input.set(0,key); 
            assertEquals(expected, searchTermExtractor.exec(input));
            testCount++;
        }
        assertEquals(tests.size(), testCount);
    }
}
