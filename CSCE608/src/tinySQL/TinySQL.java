package tinySQL;
import java.io.IOException;
import java.io.PrintStream;
import jline.TerminalFactory;
import jline.console.ConsoleReader;
import jline.console.completer.FileNameCompleter;

public class TinySQL {
	
    protected static PrintStream out = System.out;
    protected static void welcome() {
        out.println();
        out.println("This is for CSCE project CS608!");
        out.println("http://faculty.cs.tamu.edu/chen/");
        out.println("Please Enter the TinySQL command:");
        out.println();
        return;
    }
    public static void main(String[] args) {
        try {
        	welcome();
            ConsoleReader console = new ConsoleReader();
            console.addCompleter(new FileNameCompleter());
            console.setPrompt("prompt> ");
            String line = null;
            PhiQuery query = new PhiQuery();
            while ((line = console.readLine()) != null) {
            	 //console.println(line);
                query.execute(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                TerminalFactory.get().restore();
                System.out.println("Bye!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
