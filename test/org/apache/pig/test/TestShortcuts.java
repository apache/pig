package org.apache.pig.test;

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.grunt.Grunt;
import org.apache.pig.tools.pigscript.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

public class TestShortcuts {
    private String basedir = "test/org/apache/pig/test/data";
    private Data data;
    private PigServer server;
    private PigContext context;

    @Before
    public void setup() throws ExecException, IOException {
        server = new PigServer("local");
        context = server.getPigContext();
        data = resetData(server);
        data.set("input", tuple("dog", "miami", 12), tuple("cat", "miami", 18), tuple("turtle", "tampa", 4),
                tuple("dog", "tampa", 14), tuple("cat", "naples", 9), tuple("dog", "naples", 5),
                tuple("turtle", "naples", 1));
    }

    @Test
    public void testExplainShortcut() throws Throwable {
        String cmd = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = foreach a generate $0 as foo;" + "\\e b;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    /**
     * When no alias is passed, last alias should be explained
     *
     * @throws Throwable
     */
    @Test
    public void testExplainShortcutNoAlias() throws Throwable {
        String cmd = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = foreach a generate $0 as foo;" + "\\e;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    /**
     * When no alias is defined earlier, exception is expected
     *
     * @throws Throwable
     */
    @Test(expected = ParseException.class)
    public void testExplainShortcutNoAliasDefined() throws Throwable {
        String cmd = "\\e";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    @Test
    public void testExplainShortcutScript() throws Throwable {
        String cmd = "\\e -script " + basedir + "/explainScript.pig;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    @Test
    public void testDescribeShortcut() throws Throwable {
        String cmd = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = foreach a generate $0 as foo;" + "\\de b;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    /**
     * When no alias is passed, last alias should be described
     *
     * @throws Throwable
     */
    @Test
    public void testDescribeShortcutNoAlias() throws Throwable {
        String cmd = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = foreach a generate $0 as foo;" + "\\de;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    /**
     * When no alias is define, then exception is expected
     *
     * @throws Throwable
     */
    @Test(expected = IOException.class)
    public void testDescribeShortcutNoAliasDefined() throws Throwable {
        String cmd = "\\de";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    @Test
    public void testDumpShortcut() throws Throwable {
        String cmd = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = foreach a generate $0 as foo;" + "\\d b;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    /**
     * When no alias is passed, last alias should be dumped
     *
     * @throws Throwable
     */
    @Test
    public void testDumpShortcutNoAlias() throws Throwable {
        String cmd = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = foreach a generate $0 as foo;" + "\\d;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    /**
     * When no alias is defined, exception is expected
     *
     * @throws Throwable
     */
    @Test(expected = IOException.class)
    public void testDumpShortcutNoAliasDefined() throws Throwable {
        String cmd = "\\d";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    @Test
    public void testIllustrateShortcut() throws Throwable {
        String cmd = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = group a by $0;" + "\\i b;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    /**
     * When no alias is passed, last alias should be illustrated
     *
     * @throws Throwable
     */
    @Test
    public void testIllustrateShortcutNoAlias() throws Throwable {
        String cmd = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = group a by $0;" + "\\i";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    /**
     * When no alias is defined, exception is expected
     *
     * @throws Throwable
     */
    @Test(expected = ParseException.class)
    public void testIllustrateShortcutNoAliasDefined() throws Throwable {
        String cmd = "\\i";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    @Test
    public void testIllustrateShortcutScript() throws Throwable {
        String cmd = "\\i -script " + basedir + "/illustrate.pig;";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }

    @Test
    public void testQuit() throws Throwable {
        String cmd = "a = load 'input' USING mock.Storage() as (x:chararray,y:chararray,z:long);"
                + "b = group a by $0;" + "\\q";

        ByteArrayInputStream cmdstream = new ByteArrayInputStream(cmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmdstream);

        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        grunt.exec();
    }
}
