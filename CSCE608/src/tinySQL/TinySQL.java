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
        out.println("Please Enter the TinySQL command:");
        out.println();
        return;
    }
    public static void main(String[] args) throws ParserException {
    	
        try {
        	PhiQuery query = new PhiQuery();
        	if (args.length > 0) {
        		query.parseFile(args);
        		return;
        	}
        	welcome();
            ConsoleReader console = new ConsoleReader();
            console.addCompleter(new FileNameCompleter());
            console.setPrompt("prompt> ");
            String line = null;
            while ((line=console.readLine()) != null) {
            	try {
                query.execute(line);
                }catch(ParserException e) {
                	System.out.println(e);
                }catch (IOException e) {
                	System.out.println(e);
                }catch (Exception e) {
                	System.out.println("Syntax Error!");
                }
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
