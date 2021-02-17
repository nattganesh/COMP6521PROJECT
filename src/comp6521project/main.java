package comp6521project;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class main {

	public static void main(String[] args) throws ParseException {
		// Test for writer
		Tuple tuple=new Tuple(1377,
				"Kathye Bardey",
				0,
				121001410,
				"Smoaks SC 29481 South",
				3632119,
				new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-27"));
		Block block = new Block();
		block.addTuple(tuple);
		Writer writer = new Writer("./bin/writer_test.txt");
		writer.write(block);
		writer.close();
	}

}
