package tinySQL;
/*
 * This is straight from:
 * https://github.com/jline/jline2/blob/master/src/main/java/jline/console/internal/ConsoleReaderInputStream.java
 *
 * RA has to replicate the code here because jline2 makes this class
 * internal to its package (as of Aug. 15, 2014).
 */

import jline.console.ConsoleReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.SequenceInputStream;
import java.util.Enumeration;

import jargs.gnu.CmdLineParser;

/**
 * An {@link InputStream} implementation that wraps a {@link ConsoleReader}.
 * It is useful for setting up the {@link System#in} for a generic console.
 *
 * @author <a href="mailto:mwp1@cornell.edu">Marc Prud'hommeaux</a>
 * @since 2.7
 */
class ConsoleReaderInputStream
    extends SequenceInputStream
{
    private static InputStream systemIn = System.in;
    protected static PrintStream err = System.err;
    protected static PrintStream out = System.out;
    public static void setIn() throws IOException {
        setIn(new ConsoleReader());
    }
 
    public static void setIn(final ConsoleReader reader) {
        System.setIn(new ConsoleReaderInputStream(reader));
    }
 
    /**
     * Restore the original {@link System#in} input stream.
     */
    public static void restoreIn() {
        System.setIn(systemIn);
    }
 
    public ConsoleReaderInputStream(final ConsoleReader reader) {
        super(new ConsoleEnumeration(reader));
    }
    protected static void welcome() {
        out.println();
        out.println("This is for CSCE project CS608!");
        out.println("http://faculty.cs.tamu.edu/chen/");
        out.println();
        return;
    }
    protected static void usage() {
        out.println("Usage: ra [Options] [PROPS_FILE]");
        out.println("Options:");
        out.println("  -h: print this message, and exit");
        out.println("  -i FILE: read commands from FILE instead of standard input");
        out.println("  -o FILE: save a transcript of the session in FILE");
        out.println();
        return;
    }
    protected static void exit() {
        out.println("Bye!");
        out.println();
        exit(0);
        return;
    }
    protected static void exit(int code) {
        System.exit(code);
    }

    private static class ConsoleEnumeration
        implements Enumeration
    {
        private final ConsoleReader reader;
        private ConsoleLineInputStream next = null;
        private ConsoleLineInputStream prev = null;
 
        public ConsoleEnumeration(final ConsoleReader reader) {
            this.reader = reader;
        }
 
        public Object nextElement() {
            if (next != null) {
                InputStream n = next;
                prev = next;
                next = null;
 
                return n;
            }
 
            return new ConsoleLineInputStream(reader);
        }
 
        public boolean hasMoreElements() {
            // the last line was null
            if ((prev != null) && (prev.wasNull == true)) {
                return false;
            }
 
            if (next == null) {
                next = (ConsoleLineInputStream) nextElement();
            }
 
            return next != null;
        }
    }
 
    private static class ConsoleLineInputStream
        extends InputStream
    {
        private final ConsoleReader reader;
        private String line = null;
        private int index = 0;
        private boolean eol = false;
        protected boolean wasNull = false;
 
        public ConsoleLineInputStream(final ConsoleReader reader) {
            this.reader = reader;
        }
 
        public int read() throws IOException {
            if (eol) {
                return -1;
            }
 
            if (line == null) {
                line = reader.readLine();
            }
 
            if (line == null) {
                wasNull = true;
                return -1;
            }
 
            if (index >= line.length()) {
                eol = true;
                return '\n'; // lines are ended with a newline
            }
 
            return line.charAt(index++);
        }
    }
    public static void main(String[] args) {

        welcome();
        CmdLineParser cmdLineParser = new CmdLineParser();
        CmdLineParser.Option helpO = cmdLineParser.addBooleanOption('h', "help");
        CmdLineParser.Option inputO = cmdLineParser.addStringOption('i', "input");
        CmdLineParser.Option outputO = cmdLineParser.addStringOption('o', "output");
        CmdLineParser.Option passwordO = cmdLineParser.addStringOption('p', "password");
        CmdLineParser.Option promptPasswordO = cmdLineParser.addBooleanOption('P', "prompt-password");
        CmdLineParser.Option schemaO = cmdLineParser.addStringOption('s', "schema");
        CmdLineParser.Option urlO = cmdLineParser.addStringOption('l', "url");
        CmdLineParser.Option userO = cmdLineParser.addStringOption('u', "user");
        CmdLineParser.Option verboseO = cmdLineParser.addBooleanOption('v', "verbose");
        try {
            cmdLineParser.parse(args);
        } catch (CmdLineParser.OptionException e) {
            err.println(e.getMessage());
            usage();
            exit(1);
        }
        boolean help = ((Boolean)cmdLineParser.getOptionValue(helpO, Boolean.FALSE)).booleanValue();
        String inFileName = (String)cmdLineParser.getOptionValue(inputO);
        String outFileName = (String)cmdLineParser.getOptionValue(outputO);
        String password = (String)cmdLineParser.getOptionValue(passwordO);
        boolean promptPassword = ((Boolean)cmdLineParser.getOptionValue(promptPasswordO, Boolean.FALSE)).booleanValue();
        String schema = (String)cmdLineParser.getOptionValue(schemaO);
        String url = (String)cmdLineParser.getOptionValue(urlO);
        String user = (String)cmdLineParser.getOptionValue(userO);
        boolean verbose = ((Boolean)cmdLineParser.getOptionValue(verboseO, Boolean.FALSE)).booleanValue();
        if (help) {
            usage();
            exit(1);
        }
    }
}