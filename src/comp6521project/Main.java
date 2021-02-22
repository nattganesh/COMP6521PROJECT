package comp6521project;

import java.text.ParseException;

public class Main {

	public static String inputFileName = "Input_Example.txt";
//	public static String inputFileName = "sortCheck.txt";
    public static String inputPath = System.getProperty("user.dir")+"\\textfiles\\input\\";
    public static String outputPath = System.getProperty("user.dir")+"\\textfiles\\output\\";
    
    
    
	public static void main(String[] args) throws ParseException {
		// Test for writer
//		Tuple tuple=new Tuple(1377,
//				"Kathye Bardey",
//				0,
//				121001410,
//				"Smoaks SC 29481 South",
//				3632119,
//				new SimpleDateFormat("yyyy-MM-dd").parse("2021-01-27"));
//		Block block = new Block();
//		block.addTuple(tuple);
//		Writer writer = new Writer("./bin/writer_test.txt");
//		writer.write(block);
//		writer.close();
		
		System.out.println(Runtime.getRuntime().maxMemory());
		
		Reader r = new Reader(inputPath + inputFileName);
		r.readBlock();
		
		quickSortByClientID(r.currentBlock, 0, r.currentBlock.records.length - 1);

		Writer writer = new Writer(outputPath + inputFileName);
		writer.write(r.currentBlock);
		writer.close();
	}
	
	public static void quickSortByClientID(Block toBeSorted, int begin, int end) 
	{
		if (begin < end) 
		{
			int partitionIndex = partition(toBeSorted, begin, end);
			quickSortByClientID(toBeSorted, begin, partitionIndex-1);
			quickSortByClientID(toBeSorted, partitionIndex+1, end);
		}
	}
    
	public static int partition(Block block, int low, int high) {
        Tuple pivot = block.getTuple(high);
        int i = (low-1);

        for (int j = low; j < high; j++) 
        {
            if (block.getTuple(j).clientId <= pivot.clientId) 
            {
                i++;

                Tuple swapTemp = block.getTuple(i);
                block.setTuple(i, block.getTuple(j));
                block.setTuple(j, swapTemp);
            }
        }

        Tuple swapTemp = block.getTuple(i+1);
        block.setTuple(i+1, block.getTuple(high));
        block.setTuple(high, swapTemp);

        return i+1;
    }
}
