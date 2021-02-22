package comp6521project;

import java.text.ParseException;

public class Main {

	public static String inputFileName = "Input_Example";
//	public static String inputFileName = "sortCheck";
	public static String fileExtension = ".txt";
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
		
		Reader r = new Reader(inputPath + inputFileName + fileExtension);
		int numBlocks = 1;
		
		while(!r.finishedReading) 
		{
			r.readBlock();
		
			quickSortByClientID(r.currentBlock, 0, r.currentBlock.records.length - 1);

			Writer writer = new Writer(outputPath + inputFileName + "_" + numBlocks + fileExtension);
			writer.write(r.currentBlock);
			writer.close();
			numBlocks++;
			
//			System.out.println("\nBlock ID = " + numBlocks + "\n");
		}
	}
	
	public static void quickSortByClientID(Block toBeSorted, int low, int high) 
	{
		if(toBeSorted.getTuple(high) == null) 
		{
			return;
		}
		if (low < high) 
		{
			int partitionIndex = partition(toBeSorted, low, high);
			quickSortByClientID(toBeSorted, low, partitionIndex-1);
			quickSortByClientID(toBeSorted, partitionIndex+1, high);
		}
	}
    
	public static int partition(Block toBeSorted, int low, int high) {
        Tuple pivot = toBeSorted.getTuple(high);
        int i = (low-1);

        for (int j = low; j < high; j++) 
        {
            if (toBeSorted.getTuple(j).clientId <= pivot.clientId) 
            {
                i++;

                Tuple swapTemp = toBeSorted.getTuple(i);
                toBeSorted.setTuple(i, toBeSorted.getTuple(j));
                toBeSorted.setTuple(j, swapTemp);
            }
        }

        Tuple swapTemp = toBeSorted.getTuple(i+1);
        toBeSorted.setTuple(i+1, toBeSorted.getTuple(high));
        toBeSorted.setTuple(high, swapTemp);

        return i+1;
    }
}
