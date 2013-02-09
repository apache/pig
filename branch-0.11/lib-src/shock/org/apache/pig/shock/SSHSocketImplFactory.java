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
package org.apache.pig.shock;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketImpl;
import java.net.SocketOptions;
import java.net.SocketImplFactory;
import java.net.UnknownHostException;
import java.net.Proxy.Type;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.jcraft.jsch.ChannelDirectTCPIP;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Logger;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SocketFactory;
import com.jcraft.jsch.UserInfo;

/**
 * This class replaces the standard SocketImplFactory with a factory
 * that will use an SSH proxy. Only connect operations are supported. There
 * are no server operations and nio.channels are not supported.
 * <p>
 * This class uses the following system properties:
 * <ul>
 * <li>user.home - used to calculate the default .ssh directory
 * <li>user.name - used for the default ssh user id
 * <li>ssh.user - will override user.name as the user name to send over ssh
 * <li>ssh.gateway - sets the ssh host that will proxy connections
 * <li>ssh.knownhosts - the known hosts file. (We will not add to it)
 * <li>ssh.identity - the location of the identity file. The default name
 *       is openid in the ssh directory. THIS FILE MUST NOT BE PASSWORD
 *       PROTECTED.
 * </ul>
 * @author breed
 *
 */
public class SSHSocketImplFactory implements SocketImplFactory, Logger {
    
    private static final Log log = LogFactory.getLog(SSHSocketImplFactory.class);

    Session session;

    public static SSHSocketImplFactory getFactory() throws JSchException, IOException {
        return getFactory(System.getProperty("ssh.gateway"));
    }
    static HashMap<String, SSHSocketImplFactory> factories = new HashMap<String, SSHSocketImplFactory>();
    public synchronized static SSHSocketImplFactory getFactory(String host) throws JSchException, IOException {
        SSHSocketImplFactory factory = factories.get(host);
        if (factory == null) {
            factory = new SSHSocketImplFactory(host);
            factories.put(host, factory);
        }
        return factory;
    }
    
    private SSHSocketImplFactory(String host) throws JSchException, IOException {
        JSch jsch = new JSch();
        jsch.setLogger(this);
        String passphrase = "";
        String defaultSSHDir = System.getProperty("user.home") + "/.ssh";
        String identityFile = defaultSSHDir + "/openid";
        String user = System.getProperty("user.name");
        user = System.getProperty("ssh.user", user);
        if (host == null) {
            throw new RuntimeException(
                    "ssh.gateway system property must be set");
        }
        String knownHosts = defaultSSHDir + "/known_hosts";
        knownHosts = System.getProperty("ssh.knownhosts", knownHosts);
        jsch.setKnownHosts(knownHosts);
        identityFile = System.getProperty("ssh.identity", identityFile);
        jsch.addIdentity(identityFile, passphrase.getBytes());
        session = jsch.getSession(user, host);
        Properties props = new Properties();
        props.put("compression.s2c", "none");
        props.put("compression.c2s", "none");
        props.put("cipher.s2c", "blowfish-cbc,3des-cbc");
        props.put("cipher.c2s", "blowfish-cbc,3des-cbc");
        if (jsch.getHostKeyRepository().getHostKey(host, null) == null) {
            // We don't have a way to prompt, so if it isn't there we want
            // it automatically added.
            props.put("StrictHostKeyChecking", "no");
        }
        session.setConfig(props);
        session.setDaemonThread(true);

        // We have to make sure that SSH uses it's own socket factory so
        // that we don't get recursion
        SocketFactory sfactory = new SSHSocketFactory();
        session.setSocketFactory(sfactory);
        UserInfo userinfo = null;
        session.setUserInfo(userinfo);
        session.connect();
        if (!session.isConnected()) {
            throw new IOException("Session not connected");
        }
    }

    public SocketImpl createSocketImpl() {
        return new SSHSocketImpl(session);

    }

    public boolean isEnabled(int arg0) {
        // Default to not logging anything
        return false;
    }

    public void log(int arg0, String arg1) {
        log.error(arg0 + ": " + arg1);
    }

    static class SSHProcess extends Process {
        ChannelExec channel;

        InputStream is;

        InputStream es;

        OutputStream os;

        SSHProcess(ChannelExec channel) throws IOException {
            this.channel = channel;
            is = channel.getInputStream();
            es = channel.getErrStream();
            os = channel.getOutputStream();
        }

        /* (non-Javadoc)
         * @see java.lang.Process#destroy()
         */
        @Override
        public void destroy() {
            channel.disconnect();
        }

        /* (non-Javadoc)
         * @see java.lang.Process#exitValue()
         */
        @Override
        public int exitValue() {
            return channel.getExitStatus();
        }

        /* (non-Javadoc)
         * @see java.lang.Process#getErrorStream()
         */
        @Override
        public InputStream getErrorStream() {
            return es;
        }

        /* (non-Javadoc)
         * @see java.lang.Process#getInputStream()
         */
        @Override
        public InputStream getInputStream() {
            return is;
        }

        /* (non-Javadoc)
         * @see java.lang.Process#getOutputStream()
         */
        @Override
        public OutputStream getOutputStream() {
            return os;
        }

        /* (non-Javadoc)
         * @see java.lang.Process#waitFor()
         */
        @Override
        public int waitFor() throws InterruptedException {
            while (channel.isConnected()) {
                Thread.sleep(1000);
            }
            return channel.getExitStatus();
        }

    }

    public Process ssh(String cmd) throws JSchException, IOException {
        ChannelExec channel = (ChannelExec) session.openChannel("exec");
        channel.setCommand(cmd);
        channel.setPty(true);
        channel.connect();
        return new SSHProcess(channel);
    }
}

/**
 * This socket factory is only used by SSH. We implement it using nio.channels
 * since those classes do not use the SocketImplFactory.
 * 
 * @author breed
 *
 */
class SSHSocketFactory implements SocketFactory {

    private final static Log log = LogFactory.getLog(SSHSocketFactory.class);
    
    public Socket createSocket(String host, int port) throws IOException,
            UnknownHostException {
        String socksHost = System.getProperty("socksProxyHost");
        Socket s;
        InetSocketAddress addr = new InetSocketAddress(host, port);
        if (socksHost != null) {
            Proxy proxy = new Proxy(Type.SOCKS, new InetSocketAddress(
                    socksHost, 1080));
            s = new Socket(proxy);
            s.connect(addr);
        } else {
            log.error(addr);
            SocketChannel sc = SocketChannel.open(addr);
            s = sc.socket();
        }
        s.setTcpNoDelay(true);
        return s;
    }

    public InputStream getInputStream(Socket socket) throws IOException {
        return new ChannelInputStream(socket.getChannel());
    }

    public OutputStream getOutputStream(Socket socket) throws IOException {
        return new ChannelOutputStream(socket.getChannel());
    }
}

class ChannelOutputStream extends OutputStream {
    SocketChannel sc;

    public ChannelOutputStream(SocketChannel sc) {
        this.sc = sc;
    }

    @Override
    public void write(int b) throws IOException {
        byte bs[] = new byte[1];
        bs[0] = (byte) b;
        write(bs);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
        sc.write(ByteBuffer.wrap(b, off, len));
    }

}

class ChannelInputStream extends InputStream {
    SocketChannel sc;

    public ChannelInputStream(SocketChannel sc) {
        this.sc = sc;
    }

    @Override
    public int read() throws IOException {
        byte b[] = new byte[1];
        if (read(b) != 1) {
            return -1;
        }
        return b[0] & 0xff;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        return sc.read(ByteBuffer.wrap(b, off, len));
    }
}

/**
 * We aren't going to actually create any new connection, we will forward
 * things to SSH.
 */
class SSHSocketImpl extends SocketImpl {
    
    private static final Log log = LogFactory.getLog(SSHSocketImpl.class);

    Session session;

    ChannelDirectTCPIP channel;

    InputStream is;

    OutputStream os;

    SSHSocketImpl(Session session) {
        this.session = session;
    }

    @Override
    protected void accept(SocketImpl s) throws IOException {
        throw new IOException("SSHSocketImpl does not implement accept");
    }

    @Override
    protected int available() throws IOException {
        if (is == null) {
            throw new ConnectException("Not connected");
        }
        return is.available();
    }

    @Override
    protected void bind(InetAddress host, int port) throws IOException {
        if ((host != null && !host.isAnyLocalAddress()) || port != 0) {
            throw new IOException("SSHSocketImpl does not implement bind");
        }
    }

    @Override
    protected void close() throws IOException {
        if (channel != null) {
            //        channel.disconnect();
            is = null;
            os = null;
        }
    }

    public final static String defaultDomain = ".inktomisearch.com";

    @Override
    protected void connect(String host, int port) throws IOException {
        InetAddress addr = null;
        try {
            addr = InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            host += defaultDomain;
            addr = InetAddress.getByName(host);
        }
        connect(addr, port);
    }

    @Override
    protected void connect(InetAddress address, int port) throws IOException {
        connect(new InetSocketAddress(address, port), 300000);
    }

    @Override
    protected void connect(SocketAddress address, int timeout)
            throws IOException {
        try {
            if (!session.isConnected()) {
                session.connect();
            }
            channel = (ChannelDirectTCPIP) session.openChannel("direct-tcpip");
            //is = channel.getInputStream();
            //os = channel.getOutputStream();
            channel.setHost(((InetSocketAddress) address).getHostName());
            channel.setPort(((InetSocketAddress) address).getPort());
            channel.setOrgPort(22);
            is = new PipedInputStream();
            os = new PipedOutputStream();
            channel
                    .setInputStream(new PipedInputStream((PipedOutputStream) os));
            channel
                    .setOutputStream(new PipedOutputStream(
                            (PipedInputStream) is));
            channel.connect();
            if (!channel.isConnected()) {
                log.error("Not connected");
            }
            if (channel.isEOF()) {
                log.error("EOF");
            }
        } catch (JSchException e) {
            log.error(e);
            IOException newE = new IOException(e.getMessage());
            newE.setStackTrace(e.getStackTrace());
            throw newE;
        }
    }

    @Override
    protected void create(boolean stream) throws IOException {
        if (stream == false) {
            throw new IOException("Cannot handle UDP streams");
        }
    }

    @Override
    protected InputStream getInputStream() throws IOException {
        return is;
    }

    @Override
    protected OutputStream getOutputStream() throws IOException {
        return os;
    }

    @Override
    protected void listen(int backlog) throws IOException {
        throw new IOException("SSHSocketImpl does not implement listen");
    }

    @Override
    protected void sendUrgentData(int data) throws IOException {
        throw new IOException("SSHSocketImpl does not implement sendUrgentData");
    }

    public Object getOption(int optID) throws SocketException {
        if (optID == SocketOptions.SO_SNDBUF)
            return Integer.valueOf(1024);
        else
		    throw new SocketException("SSHSocketImpl does not implement getOption for " + optID);
    }

    /**
     * We silently ignore setOptions because they do happen, but there is
     * nothing that we can do about it.
     */
    public void setOption(int optID, Object value) throws SocketException {
    }

    static public void main(String args[]) {
        try {
            System.setProperty("ssh.gateway", "ucdev2");
            final SSHSocketImplFactory fac = SSHSocketImplFactory.getFactory();
            Socket.setSocketImplFactory(fac);
            for (int i = 0; i < 10; i++) {
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            log.error("Starting " + this);
                            connectTest("www.yahoo.com");
                            log.error("Finished " + this);
                        } catch (Exception e) {
                            log.error(e);
                        }
                    }
                }.start();
            }
            Thread.sleep(1000000);
            connectTest("www.news.com");
            log.info("******** Starting PART II");
            for (int i = 0; i < 10; i++) {
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            log.error("Starting " + this);
                            connectTest("www.flickr.com");
                            log.error("Finished " + this);
                        } catch (Exception e) {
                            log.error(e);
                        }
                    }
                }.start();
            }
        } catch (Exception e) {
            log.error(e);
        }
    }

    private static void connectTest(String host) throws JSchException,
            IOException {
        Socket s = new Socket(host, 80);
        s.getOutputStream().write("GET / HTTP/1.0\r\n\r\n".getBytes());
        byte b[] = new byte[80];
        int rc = s.getInputStream().read(b);
        System.out.write(b, 0, rc);
        s.close();
    }

    private static void lsTest(SSHSocketImplFactory fac) throws JSchException,
            IOException {
        Process p = fac.ssh("ls");
        byte b[] = new byte[1024];
        final InputStream es = p.getErrorStream();
        new Thread() {
            @Override
            public void run() {
                try {
                    while (es.available() > 0) {
                        es.read();
                    }
                } catch (Exception e) {
                }
            }
        }.start();
        p.getOutputStream().close();
        InputStream is = p.getInputStream();
        int rc;
        while ((rc = is.read(b)) > 0) {
            System.out.write(b, 0, rc);
        }
    }
}
