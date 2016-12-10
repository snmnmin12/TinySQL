package tinySQL;

public class ParserException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	ParserException(String error) {
		super(error);
	}

	public static void main(String[] args) throws ParserException {
		// TODO Auto-generated method stub
		throw new ParserException("I am here throw exception");
	}

}
