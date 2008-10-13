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

package org.apache.pig.piggybank.evaluation.util.apachelogparser;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Tuple;


/**
 * SearchTermExtractor takes a url string and extracts the search terms. For example, given
 * 
 * http://www.google.com/search?hl=en&safe=active&rls=GGLG,GGLG:2005-24,GGLG:en&q=purpose+of+life&btnG=Search
 * 
 * then
 * 
 * purpose of life
 * 
 * would be extracted.
 * 
 * From pig latin, usage looks something like
 * 
 * searchTerm = FOREACH row GENERATE
 * org.apache.pig.piggybank.evaluation.util.apachelogparser.SearchTermExtractor(referer);
 * 
 * Supported search engines include alltheweb.com, altavista.com, aolsearch.aol.com, arianna.libero.it,
 * as.starware.com, ask.com, blogs.icerocket.com, blueyonder.co.uk, busca.orange.es, buscador.lycos.es,
 * buscador.terra.es, buscar.ozu.es, categorico.it, cerca.lycos.it, cuil.com, excite.it, godado.com,
 * godado.it, gps.virgin.net, hotbot.com, ilmotore.com, it.altavista.com, ithaki.net, libero.it, lycos.es,
 * lycos.it, mamma.com, megasearching.net, mirago.co.uk, netscape.com, ozu.es, ricerca.alice.it,
 * search.aol.co.uk, search.bbc.co.uk, search.conduit.com, search.icq.com, search.live.com,
 * search.lycos.co.uk, search.lycos.com, search.msn.co.uk, search.msn.com, search.myway.com,
 * search.mywebsearch.com, search.ntlworld.com, search.orange.co.uk, search.sweetim.com,
 * search.virginmedia.com, simpatico.ws, soso.com, suche.fireball.de, suche.web.de, terra.es, tesco.net,
 * thespider.it, tiscali.co.uk, uk.altavista.com, uk.ask.com
 * 
 * Thanks to Spiros Denaxas for his URI::ParseSearchString, which is the basis for the lookups.
 */
public class SearchTermExtractor extends EvalFunc<DataAtom> {
    private static Matcher TERM_MATCHER = null;
    private static Matcher P_TERM_MATCHER = null;

    static {
        TERM_MATCHER = Pattern.compile("\\b(?:q|buscar|key|qry|qs|query|s|searchfor|su|w)=([^&]+)").matcher("");
        P_TERM_MATCHER = Pattern.compile("\\bp=([^&]+)").matcher("");
    }

    private String myDecode(String string) {
        try {
            string = URLDecoder.decode(string, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return string;
    }

    private static HashMap<String, Boolean> HOSTS = new HashMap<String, Boolean>();
    static {
        HOSTS.put("alltheweb.com", true);
        HOSTS.put("altavista.com", true);
        HOSTS.put("aolsearch.aol.com", true);
        HOSTS.put("arianna.libero.it", true);
        HOSTS.put("as.starware.com", true);
        HOSTS.put("ask.com", true);
        HOSTS.put("blogs.icerocket.com", true);
        HOSTS.put("blueyonder.co.uk", true);
        HOSTS.put("busca.orange.es", true);
        HOSTS.put("buscador.lycos.es", true);
        HOSTS.put("buscador.terra.es", true);
        HOSTS.put("buscar.ozu.es", true);
        HOSTS.put("categorico.it", true);
        HOSTS.put("cerca.lycos.it", true);
        HOSTS.put("cuil.com", true);
        HOSTS.put("excite.it", true);
        HOSTS.put("godado.com", true);
        HOSTS.put("godado.it", true);
        HOSTS.put("gps.virgin.net", true);
        HOSTS.put("hotbot.com", true);
        HOSTS.put("ilmotore.com", true);
        HOSTS.put("it.altavista.com", true);
        HOSTS.put("ithaki.net", true);
        HOSTS.put("libero.it", true);
        HOSTS.put("lycos.es", true);
        HOSTS.put("lycos.it", true);
        HOSTS.put("mamma.com", true);
        HOSTS.put("megasearching.net", true);
        HOSTS.put("mirago.co.uk", true);
        HOSTS.put("netscape.com", true);
        HOSTS.put("ozu.es", true);
        HOSTS.put("ricerca.alice.it", true);
        HOSTS.put("search.aol.co.uk", true);
        HOSTS.put("search.bbc.co.uk", true);
        HOSTS.put("search.conduit.com", true);
        HOSTS.put("search.icq.com", true);
        HOSTS.put("search.live.com", true);
        HOSTS.put("search.lycos.co.uk", true);
        HOSTS.put("search.lycos.com", true);
        HOSTS.put("search.msn.co.uk", true);
        HOSTS.put("search.msn.com", true);
        HOSTS.put("search.myway.com", true);
        HOSTS.put("search.mywebsearch.com", true);
        HOSTS.put("search.ntlworld.com", true);
        HOSTS.put("search.orange.co.uk", true);
        HOSTS.put("search.sweetim.com", true);
        HOSTS.put("search.virginmedia.com", true);
        HOSTS.put("simpatico.ws", true);
        HOSTS.put("soso.com", true);
        HOSTS.put("suche.fireball.de", true);
        HOSTS.put("suche.web.de", true);
        HOSTS.put("terra.es", true);
        HOSTS.put("tesco.net", true);
        HOSTS.put("thespider.it", true);
        HOSTS.put("tiscali.co.uk", true);
        HOSTS.put("uk.altavista.com", true);
        HOSTS.put("uk.ask.com", true);
    }

    @Override
    public void exec(Tuple input, DataAtom output) {
        String url = input.getAtomField(0).strval();

        if (url == null)
            return;

        URL urlObject = null;
        try {
            urlObject = new URL(url);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        if (urlObject == null)
            return;

        String host = urlObject.getHost();
        if (host == null)
            return;

        host = host.replaceFirst("^www\\.", "");
        if (host == null)
            return;

        host = host.toLowerCase();

        if (HOSTS.containsKey(host) || host.contains("google.co") || host.contains("search.yahoo")) {
            String queryString = urlObject.getQuery();

            TERM_MATCHER.reset(queryString);
            if (TERM_MATCHER.find()) {
                String terms = TERM_MATCHER.group(1);
                output.setValue(myDecode(terms));

                // at least once, a p= comes before a q= when p= isn't tied to the search terms
            } else {
                P_TERM_MATCHER.reset(queryString);
                if (P_TERM_MATCHER.find()) {
                    String terms = P_TERM_MATCHER.group(1);
                    output.setValue(myDecode(terms));
                }
            }
            return;
        }

        if (host.endsWith("feedster.com") || host.endsWith("technorati.com")) {
            String path = urlObject.getPath();
            if (path == null)
                return;

            path = path.replaceFirst("^/search/", "");
            output.setValue(myDecode(path));
        }
    }
}
