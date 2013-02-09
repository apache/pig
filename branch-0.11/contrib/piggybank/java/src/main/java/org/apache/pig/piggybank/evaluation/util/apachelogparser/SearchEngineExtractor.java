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

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * SearchEngineExtractor takes a url string and extracts the search engine. For example, given
 * 
 * http://www.google.com/search?hl=en&safe=active&rls=GGLG,GGLG:2005-24,GGLG:en&q=purpose+of+life&btnG=Search
 * 
 * then
 * 
 * Google
 * 
 * would be extracted.
 * 
 * From pig latin, usage looks something like
 * 
 * searchEngine = FOREACH row GENERATE
 * org.apache.pig.piggybank.evaluation.util.apachelogparser.SearchEngineExtractor(referer);
 * 
 * Supported search engines include abacho.com, alice.it, alltheweb.com, altavista.com, aolsearch.aol.com,
 * as.starware.com, ask.com, blogs.icerocket.com, blogsearch.google.com, blueyonder.co.uk, busca.orange.es,
 * buscador.lycos.es, buscador.terra.es, buscar.ozu.es, categorico.it, cuil.com, excite.com, excite.it,
 * fastweb.it, feedster.com, godado.com, godado.it, google.ad, google.ae, google.af, google.ag, google.am,
 * google.as, google.at, google.az, google.ba, google.be, google.bg, google.bi, google.biz, google.bo,
 * google.bs, google.bz, google.ca, google.cc, google.cd, google.cg, google.ch, google.ci, google.cl,
 * google.cn, google.co.at , google.co.bi, google.co.bw, google.co.ci, google.co.ck, google.co.cr,
 * google.co.gg, google.co.gl, google.co.gy, google.co.hu, google.co.id, google.co.il, google.co.im,
 * google.co.in, google.co.it, google.co.je, google.co.jp, google.co.ke, google.co.kr, google.co.ls,
 * google.co.ma, google.co.mu, google.co.mw, google.co.nz, google.co.pn, google.co.th, google.co.tt,
 * google.co.ug, google.co.uk, google.co.uz, google.co.ve, google.co.vi, google.co.za, google.co.zm,
 * google.co.zw, google.com, google.com.af, google.com.ag, google.com.ai, google.com.ar, google.com.au,
 * google.com.az, google.com.bd, google.com.bh, google.com.bi, google.com.bn, google.com.bo, google.com.br,
 * google.com.bs, google.com.bz, google.com.cn, google.com.co, google.com.cu, google.com.do, google.com.ec,
 * google.com.eg, google.com.et, google.com.fj, google.com.ge, google.com.gh, google.com.gi, google.com.gl,
 * google.com.gp, google.com.gr, google.com.gt, google.com.gy, google.com.hk, google.com.hn, google.com.hr,
 * google.com.jm, google.com.jo, google.com.kg, google.com.kh, google.com.ki, google.com.kz, google.com.lk,
 * google.com.lv, google.com.ly, google.com.mt, google.com.mu, google.com.mw, google.com.mx, google.com.my,
 * google.com.na, google.com.nf, google.com.ng, google.com.ni, google.com.np, google.com.nr, google.com.om,
 * google.com.pa, google.com.pe, google.com.ph, google.com.pk, google.com.pl, google.com.pr, google.com.pt,
 * google.com.py, google.com.qa, google.com.ru, google.com.sa, google.com.sb, google.com.sc, google.com.sg,
 * google.com.sv, google.com.tj, google.com.tr, google.com.tt, google.com.tw, google.com.uy, google.com.uz,
 * google.com.ve, google.com.vi, google.com.vn, google.com.ws, google.cz, google.de, google.dj, google.dk ,
 * google.dm , google.ec, google.ee, google.es, google.fi, google.fm, google.fr, google.gd, google.ge,
 * google.gf, google.gg, google.gl, google.gm, google.gp, google.gr, google.gy, google.hk, google.hn,
 * google.hr, google.ht, google.hu, google.ie, google.im, google.in, google.info, google.is, google.it,
 * google.je, google.jo, google.jobs, google.jp, google.kg, google.ki, google.kz, google.la, google.li,
 * google.lk, google.lt, google.lu, google.lv, google.ma, google.md, google.mn, google.mobi, google.ms,
 * google.mu, google.mv, google.mw, google.net, google.nf, google.nl, google.no, google.nr, google.nu,
 * google.off.ai, google.ph, google.pk, google.pl, google.pn, google.pr, google.pt, google.ro, google.ru,
 * google.rw, google.sc, google.se, google.sg, google.sh, google.si, google.sk, google.sm, google.sn,
 * google.sr, google.st, google.tk, google.tm, google.to, google.tp, google.tt, google.tv, google.tw,
 * google.ug, google.us, google.uz, google.vg, google.vn, google.vu, google.ws, gps.virgin.net, hotbot.com,
 * ilmotore.com, ithaki.net, kataweb.it, libero.it, lycos.it, mamma.com, megasearching.net, mirago.co.uk,
 * netscape.com, search.aol.co.uk, search.arabia.msn.com, search.bbc.co.uk, search.conduit.com,
 * search.icq.com, search.live.com, search.lycos.co.uk, search.lycos.com, search.msn.co.uk, search.msn.com,
 * search.myway.com, search.mywebsearch.com, search.ntlworld.com, search.orange.co.uk, search.prodigy.msn.com,
 * search.sweetim.com, search.virginmedia.com, search.yahoo.co.jp, search.yahoo.com, search.yahoo.jp,
 * simpatico.ws, soso.com, suche.fireball.de, suche.t-online.de, suche.web.de, technorati.com, tesco.net,
 * thespider.it, tiscali.co.uk, uk.altavista.com, uk.ask.com, uk.search.yahoo.com
 * 
 * Thanks to Spiros Denaxas for his URI::ParseSearchString, which is the basis for the lookups.
 */

public class SearchEngineExtractor extends EvalFunc<String> {
    private static HashMap<String, String> searchEngines = new HashMap<String, String>();
    static {
        searchEngines.put("abacho.com", "Abacho");
        searchEngines.put("alice.it", "Alice.it");
        searchEngines.put("alltheweb.com", "AllTheWeb");
        searchEngines.put("altavista.com", "Altavista");
        searchEngines.put("aolsearch.aol.com", "AOL Search");
        searchEngines.put("as.starware.com", "Starware");
        searchEngines.put("ask.com", "Ask dot com");
        searchEngines.put("blogs.icerocket.com", "IceRocket");
        searchEngines.put("blogsearch.google.com", "Google Blogsearch");
        searchEngines.put("blueyonder.co.uk", "Blueyonder");
        searchEngines.put("busca.orange.es", "Orange ES");
        searchEngines.put("buscador.lycos.es", "Lycos ES");
        searchEngines.put("buscador.terra.es", "Terra ES");
        searchEngines.put("buscar.ozu.es", "Ozu ES");
        searchEngines.put("categorico.it", "Categorico IT");
        searchEngines.put("cuil.com", "Cuil");
        searchEngines.put("excite.com", "Excite");
        searchEngines.put("excite.it", "Excite IT");
        searchEngines.put("fastweb.it", "Fastweb IT");
        searchEngines.put("feedster.com", "Feedster");
        searchEngines.put("godado.com", "Godado");
        searchEngines.put("godado.it", "Godado (IT)");
        searchEngines.put("google.ad", "Google Andorra");
        searchEngines.put("google.ae", "Google United Arab Emirates");
        searchEngines.put("google.af", "Google Afghanistan");
        searchEngines.put("google.ag", "Google Antiqua and Barbuda");
        searchEngines.put("google.am", "Google Armenia");
        searchEngines.put("google.as", "Google American Samoa");
        searchEngines.put("google.at", "Google Austria");
        searchEngines.put("google.az", "Google Azerbaijan");
        searchEngines.put("google.ba", "Google Bosnia and Herzegovina");
        searchEngines.put("google.be", "Google Belgium");
        searchEngines.put("google.bg", "Google Bulgaria");
        searchEngines.put("google.bi", "Google Burundi");
        searchEngines.put("google.biz", "Google dot biz");
        searchEngines.put("google.bo", "Google Bolivia");
        searchEngines.put("google.bs", "Google Bahamas");
        searchEngines.put("google.bz", "Google Belize");
        searchEngines.put("google.ca", "Google Canada");
        searchEngines.put("google.cc", "Google Cocos Islands");
        searchEngines.put("google.cd", "Google Dem Rep of Congo");
        searchEngines.put("google.cg", "Google Rep of Congo");
        searchEngines.put("google.ch", "Google Switzerland");
        searchEngines.put("google.ci", "Google Cote dIvoire");
        searchEngines.put("google.cl", "Google Chile");
        searchEngines.put("google.cn", "Google China");
        searchEngines.put("google.co.at ", "Google Austria");
        searchEngines.put("google.co.bi", "Google Burundi");
        searchEngines.put("google.co.bw", "Google Botswana");
        searchEngines.put("google.co.ci", "Google Ivory Coast");
        searchEngines.put("google.co.ck", "Google Cook Islands");
        searchEngines.put("google.co.cr", "Google Costa Rica");
        searchEngines.put("google.co.gg", "Google Guernsey");
        searchEngines.put("google.co.gl", "Google Greenland");
        searchEngines.put("google.co.gy", "Google Guyana");
        searchEngines.put("google.co.hu", "Google Hungary ");
        searchEngines.put("google.co.id", "Google Indonesia");
        searchEngines.put("google.co.il", "Google Israel");
        searchEngines.put("google.co.im", "Google Isle of Man");
        searchEngines.put("google.co.in", "Google India");
        searchEngines.put("google.co.it", "Google Italy");
        searchEngines.put("google.co.je", "Google Jersey");
        searchEngines.put("google.co.jp", "Google Japan");
        searchEngines.put("google.co.ke", "Google Kenya");
        searchEngines.put("google.co.kr", "Google South Korea");
        searchEngines.put("google.co.ls", "Google Lesotho");
        searchEngines.put("google.co.ma", "Google Morocco");
        searchEngines.put("google.co.mu", "Google Mauritius");
        searchEngines.put("google.co.mw", "Google Malawi");
        searchEngines.put("google.co.nz", "Google New Zeland");
        searchEngines.put("google.co.pn", "Google Pitcairn Islands");
        searchEngines.put("google.co.th", "Google Thailand");
        searchEngines.put("google.co.tt", "Google Trinidad and Tobago");
        searchEngines.put("google.co.ug", "Google Uganda");
        searchEngines.put("google.co.uk", "Google UK");
        searchEngines.put("google.co.uz", "Google Uzbekistan");
        searchEngines.put("google.co.ve", "Google Venezuela");
        searchEngines.put("google.co.vi", "Google US Virgin Islands");
        searchEngines.put("google.co.za", "Google   South Africa ");
        searchEngines.put("google.co.zm", "Google Zambia");
        searchEngines.put("google.co.zw", "Google Zimbabwe");
        searchEngines.put("google.com", "Google");
        searchEngines.put("google.com.af", "Google Afghanistan");
        searchEngines.put("google.com.ag", "Google Antiqua and Barbuda");
        searchEngines.put("google.com.ai", "Google Anguilla");
        searchEngines.put("google.com.ar", "Google Argentina");
        searchEngines.put("google.com.au", "Google Australia");
        searchEngines.put("google.com.az", "Google Azerbaijan ");
        searchEngines.put("google.com.bd", "Google Bangladesh");
        searchEngines.put("google.com.bh", "Google Bahrain");
        searchEngines.put("google.com.bi", "Google Burundi");
        searchEngines.put("google.com.bn", "Google Brunei Darussalam");
        searchEngines.put("google.com.bo", "Google Bolivia ");
        searchEngines.put("google.com.br", "Google Brazil");
        searchEngines.put("google.com.bs", "Google Bahamas");
        searchEngines.put("google.com.bz", "Google Belize");
        searchEngines.put("google.com.cn", "Google China");
        searchEngines.put("google.com.co", "Google ");
        searchEngines.put("google.com.cu", "Google Cuba");
        searchEngines.put("google.com.do", "Google Dominican Rep");
        searchEngines.put("google.com.ec", "Google Ecuador");
        searchEngines.put("google.com.eg", "Google Egypt");
        searchEngines.put("google.com.et", "Google Ethiopia");
        searchEngines.put("google.com.fj", "Google Fiji");
        searchEngines.put("google.com.ge", "Google Georgia");
        searchEngines.put("google.com.gh", "Google Ghana");
        searchEngines.put("google.com.gi", "Google Gibraltar");
        searchEngines.put("google.com.gl", "Google Greenland");
        searchEngines.put("google.com.gp", "Google Guadeloupe");
        searchEngines.put("google.com.gr", "Google Greece");
        searchEngines.put("google.com.gt", "Google Guatemala");
        searchEngines.put("google.com.gy", "Google Guyana");
        searchEngines.put("google.com.hk", "Google Hong Kong");
        searchEngines.put("google.com.hn", "Google Honduras");
        searchEngines.put("google.com.hr", "Google Croatia");
        searchEngines.put("google.com.jm", "Google Jamaica");
        searchEngines.put("google.com.jo", "Google Jordan");
        searchEngines.put("google.com.kg", "Google Kyrgyzstan");
        searchEngines.put("google.com.kh", "Google Cambodia");
        searchEngines.put("google.com.ki", "Google Kiribati");
        searchEngines.put("google.com.kz", "Google Kazakhstan");
        searchEngines.put("google.com.lk", "Google Sri Lanka");
        searchEngines.put("google.com.lv", "Google Latvia");
        searchEngines.put("google.com.ly", "Google Libya");
        searchEngines.put("google.com.mt", "Google Malta");
        searchEngines.put("google.com.mu", "Google Mauritius");
        searchEngines.put("google.com.mw", "Google Malawi");
        searchEngines.put("google.com.mx", "Google Mexico");
        searchEngines.put("google.com.my", "Google Malaysia");
        searchEngines.put("google.com.na", "Google Namibia");
        searchEngines.put("google.com.nf", "Google Norfolk Island");
        searchEngines.put("google.com.ng", "Google Nigeria");
        searchEngines.put("google.com.ni", "Google Nicaragua");
        searchEngines.put("google.com.np", "Google Nepal");
        searchEngines.put("google.com.nr", "Google Nauru");
        searchEngines.put("google.com.om", "Google Oman");
        searchEngines.put("google.com.pa", "Google Panama");
        searchEngines.put("google.com.pe", "Google Peru");
        searchEngines.put("google.com.ph", "Google Philipines");
        searchEngines.put("google.com.pk", "Google Pakistan");
        searchEngines.put("google.com.pl", "Google Poland");
        searchEngines.put("google.com.pr", "Google Puerto Rico");
        searchEngines.put("google.com.pt", "Google Portugal");
        searchEngines.put("google.com.py", "Google Paraguay");
        searchEngines.put("google.com.qa", "Google ");
        searchEngines.put("google.com.ru", "Google Russia");
        searchEngines.put("google.com.sa", "Google Saudi Arabia");
        searchEngines.put("google.com.sb", "Google Solomon Islands");
        searchEngines.put("google.com.sc", "Google Seychelles");
        searchEngines.put("google.com.sg", "Google Singapore");
        searchEngines.put("google.com.sv", "Google El Savador");
        searchEngines.put("google.com.tj", "Google Tajikistan");
        searchEngines.put("google.com.tr", "Google Turkey");
        searchEngines.put("google.com.tt", "Google Trinidad and Tobago");
        searchEngines.put("google.com.tw", "Google Taiwan");
        searchEngines.put("google.com.uy", "Google Uruguay");
        searchEngines.put("google.com.uz", "Google Uzbekistan ");
        searchEngines.put("google.com.ve", "Google Venezuela");
        searchEngines.put("google.com.vi", "Google US Virgin Islands");
        searchEngines.put("google.com.vn", "Google Vietnam");
        searchEngines.put("google.com.ws", "Google Samoa");
        searchEngines.put("google.cz", "Google Czech Rep");
        searchEngines.put("google.de", "Google Germany");
        searchEngines.put("google.dj", "Google Djubouti");
        searchEngines.put("google.dk ", "Google Denmark");
        searchEngines.put("google.dm ", "Google Dominica");
        searchEngines.put("google.ec", "Google Ecuador");
        searchEngines.put("google.ee", "Google Estonia");
        searchEngines.put("google.es", "Google Spain");
        searchEngines.put("google.fi", "Google Finland");
        searchEngines.put("google.fm", "Google Micronesia");
        searchEngines.put("google.fr", "Google France");
        searchEngines.put("google.gd", "Google Grenada");
        searchEngines.put("google.ge", "Google Georgia");
        searchEngines.put("google.gf", "Google French Guiana");
        searchEngines.put("google.gg", "Google Guernsey");
        searchEngines.put("google.gl", "Google Greenland");
        searchEngines.put("google.gm", "Google Gambia");
        searchEngines.put("google.gp", "Google Guadeloupe");
        searchEngines.put("google.gr", "Google Greece");
        searchEngines.put("google.gy", "Google Guyana");
        searchEngines.put("google.hk", "Google Hong Kong");
        searchEngines.put("google.hn", "Google Honduras");
        searchEngines.put("google.hr", "Google Croatia");
        searchEngines.put("google.ht", "Google Haiti");
        searchEngines.put("google.hu", "Google Hungary");
        searchEngines.put("google.ie", "Google Ireland");
        searchEngines.put("google.im", "Google Isle of Man");
        searchEngines.put("google.in", "Google India");
        searchEngines.put("google.info", "Google dot info");
        searchEngines.put("google.is", "Google Iceland");
        searchEngines.put("google.it", "Google Italy");
        searchEngines.put("google.je", "Google Jersey");
        searchEngines.put("google.jo", "Google Jordan");
        searchEngines.put("google.jobs", "Google dot jobs");
        searchEngines.put("google.jp", "Google Japan");
        searchEngines.put("google.kg", "Google Kyrgyzstan");
        searchEngines.put("google.ki", "Google Kiribati");
        searchEngines.put("google.kz", "Google Kazakhstan");
        searchEngines.put("google.la", "Google Laos");
        searchEngines.put("google.li", "Google Liechtenstein");
        searchEngines.put("google.lk", "Google Sri Lanka");
        searchEngines.put("google.lt", "Google Lithuania");
        searchEngines.put("google.lu", "Google Luxembourg");
        searchEngines.put("google.lv", "Google Latvia");
        searchEngines.put("google.ma", "Google Morocco");
        searchEngines.put("google.md", "Google Moldova");
        searchEngines.put("google.mn", "Google Mongolia");
        searchEngines.put("google.mobi", "Google dot mobi");
        searchEngines.put("google.ms", "Google Montserrat");
        searchEngines.put("google.mu", "Google Mauritius");
        searchEngines.put("google.mv", "Google Maldives");
        searchEngines.put("google.mw", "Google Malawi");
        searchEngines.put("google.net", "Google dot net");
        searchEngines.put("google.nf", "Google Norfolk Island");
        searchEngines.put("google.nl", "Google Netherlands");
        searchEngines.put("google.no", "Google Norway");
        searchEngines.put("google.nr", "Google Nauru");
        searchEngines.put("google.nu", "Google Niue");
        searchEngines.put("google.off.ai", "Google Anguilla");
        searchEngines.put("google.ph", "Google Philipines");
        searchEngines.put("google.pk", "Google Pakistan");
        searchEngines.put("google.pl", "Google Poland");
        searchEngines.put("google.pn", "Google Pitcairn Islands");
        searchEngines.put("google.pr", "Google Puerto Rico");
        searchEngines.put("google.pt", "Google Portugal");
        searchEngines.put("google.ro", "Google Romania");
        searchEngines.put("google.ru", "Google Russia");
        searchEngines.put("google.rw", "Google Rwanda");
        searchEngines.put("google.sc", "Google Seychelles");
        searchEngines.put("google.se", "Google Sweden");
        searchEngines.put("google.sg", "Google Singapore");
        searchEngines.put("google.sh", "Google Saint Helena");
        searchEngines.put("google.si", "Google Slovenia");
        searchEngines.put("google.sk", "Google Slovakia");
        searchEngines.put("google.sm", "Google San Marino");
        searchEngines.put("google.sn", "Google Senegal");
        searchEngines.put("google.sr", "Google Suriname");
        searchEngines.put("google.st", "Google Sao Tome ");
        searchEngines.put("google.tk", "Google Tokelau");
        searchEngines.put("google.tm", "Google Turkmenistan");
        searchEngines.put("google.to", "Google Tonga");
        searchEngines.put("google.tp", "Google East Timor");
        searchEngines.put("google.tt", "Google Trinidad and Tobago");
        searchEngines.put("google.tv", "Google Tuvalu");
        searchEngines.put("google.tw", "Google Taiwan");
        searchEngines.put("google.ug", "Google Uganda");
        searchEngines.put("google.us", "Google US");
        searchEngines.put("google.uz", "Google Uzbekistan");
        searchEngines.put("google.vg", "Google British Virgin Islands");
        searchEngines.put("google.vn", "Google Vietnam");
        searchEngines.put("google.vu", "Google Vanuatu");
        searchEngines.put("google.ws", "Google Samoa");
        searchEngines.put("gps.virgin.net", "Virgin Search");
        searchEngines.put("hotbot.com", "HotBot");
        searchEngines.put("ilmotore.com", "ilMotore");
        searchEngines.put("ithaki.net", "Ithaki");
        searchEngines.put("kataweb.it", "Kataweb IT");
        searchEngines.put("libero.it", "Libero IT");
        searchEngines.put("lycos.it", "Lycos IT");
        searchEngines.put("mamma.com", "Mamma");
        searchEngines.put("megasearching.net", "Megasearching");
        searchEngines.put("mirago.co.uk", "Mirago UK");
        searchEngines.put("netscape.com", "Netscape");
        searchEngines.put("search.aol.co.uk", "AOL UK");
        searchEngines.put("search.arabia.msn.com", "MSN Arabia");
        searchEngines.put("search.bbc.co.uk", "BBC Search");
        searchEngines.put("search.conduit.com", "Conduit");
        searchEngines.put("search.icq.com", "ICQ dot com");
        searchEngines.put("search.live.com", "Live.com");
        searchEngines.put("search.lycos.co.uk", "Lycos UK");
        searchEngines.put("search.lycos.com", "Lycos");
        searchEngines.put("search.msn.co.uk", "MSN UK");
        searchEngines.put("search.msn.com", "MSN");
        searchEngines.put("search.myway.com", "MyWay");
        searchEngines.put("search.mywebsearch.com", "My Web Search");
        searchEngines.put("search.ntlworld.com", "NTLWorld");
        searchEngines.put("search.orange.co.uk", "Orange Search");
        searchEngines.put("search.prodigy.msn.com", "MSN Prodigy");
        searchEngines.put("search.sweetim.com", "Sweetim");
        searchEngines.put("search.virginmedia.com", "VirginMedia");
        searchEngines.put("search.yahoo.co.jp", "Yahoo Japan");
        searchEngines.put("search.yahoo.com", "Yahoo!");
        searchEngines.put("search.yahoo.jp", "Yahoo! Japan");
        searchEngines.put("simpatico.ws", "Simpatico IT");
        searchEngines.put("soso.com", "Soso");
        searchEngines.put("suche.fireball.de", "Fireball DE");
        searchEngines.put("suche.t-online.de", "T-Online");
        searchEngines.put("suche.web.de", "Suche DE");
        searchEngines.put("technorati.com", "Technorati");
        searchEngines.put("tesco.net", "Tesco Search");
        searchEngines.put("thespider.it", "TheSpider IT");
        searchEngines.put("tiscali.co.uk", "Tiscali UK");
        searchEngines.put("uk.altavista.com", "Altavista UK");
        searchEngines.put("uk.ask.com", "Ask UK");
        searchEngines.put("uk.search.yahoo.com", "Yahoo! UK");
    }

    @Override
    public String exec(Tuple input) throws IOException {
      if (input == null || input.size() == 0)
        return null;
      String referer="";
      try{
        referer = (String)input.get(0);

        String searchEngine = null;

        String host = null;
        host = new URL(referer).getHost().toLowerCase().replaceFirst("^www.", "");
        if (host != null)
          searchEngine = searchEngines.containsKey(host) ? searchEngines.get(host) : null;

          return searchEngine;  
      } catch (Exception e) {
        throw new IOException("Caught exception processing input row ", e);
      }
    }
    
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), 
            new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));

        return funcList;
    }
}
