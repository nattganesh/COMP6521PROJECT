package comp6521project;

import java.util.ArrayList;
import java.util.Comparator;

public class Main {

	public static final int kiloByte = 1024;
	public static final int megaByte = 1048576;
	public static final int maxMemory = kiloByte * 555;
//	public static final int maxMemory = 5 * megaByte; //5Mb
//	public static final int maxMemory = 10 * megaByte; //Case 1 : 10Mb
//	public static final int maxMemory = 20 * megaByte; //Case 2 : 20Mb
	public static String inputFileName = "Input_Example";
//	public static String inputFileName = "mergeCheck";
//	public static String inputFileName = "sortCheck";
	public static String fileExtension = ".txt";
    public static String inputPath = System.getProperty("user.dir")+"/textfiles/input/";
    public static String outputPath = System.getProperty("user.dir")+"/textfiles/output/";
    
    
    
    
	public static void main(String[] args) {
//		System.out.println(Runtime.getRuntime().maxMemory());
		
		Reader r = new Reader(inputPath + inputFileName + fileExtension);
		
		//Number of blocks we can process based of available memory
		
		int maxNumberOfBlocksToProcess = Math.floorDiv(maxMemory, Block.bytesPerBlock);
		
		
		
		
		int numBlocks = 0;

		while(!r.finishedReading)
		{
			r.readBlocks(maxNumberOfBlocksToProcess);
			System.out.println("Max Chunk size of " + maxNumberOfBlocksToProcess + " blocks can be read at a time.");
//			for(Tuple t: r.currentTuples) 
//			{
//				System.out.println(t.name);
//			}
			quickSortByClientID(r.currentTuples, 0, r.currentTuples.size() - 1);
//			for(Tuple t: r.currentTuples) 
//			{
//				System.out.println(t.name);
//			}
			System.out.println("Chunk Sorted");
			Writer writer = new Writer(outputPath + inputFileName + "_pass_0_" + numBlocks + fileExtension);
			
			int numRecordsInBlock = 0;
			Block b = new Block();
			ArrayList<Block> currentBlocks = new ArrayList<>();
			for(Tuple t: r.currentTuples) 
			{
//				System.out.println(t);
				if(numRecordsInBlock < Block.recordsPerBlock)
				{
					b.addTuple(t);
					numRecordsInBlock++;
					if(t == r.currentTuples.get(r.currentTuples.size() - 1)) 
					{
						currentBlocks.add(b);
					}
				}
				else 
				{
					currentBlocks.add(b);
					numRecordsInBlock = 0;
					b = new Block();
					b.addTuple(t);
					numRecordsInBlock++;
				}	
//				System.out.println(numRecordsInBlock);
			}
			
			writer.writeChunk(currentBlocks);
			writer.close();
			numBlocks++;
			
//			System.out.println("\nBlock ID = " + numBlocks + "\n");
		}
		System.out.println("Number of Tuples " + Reader.totalNumberOfTuples);
		merge(numBlocks);
		System.out.println("Complete");
	}
	
	public static void quickSortByClientID(ArrayList<Tuple> toBeSorted, int low, int high) 
	{
		if (low < high) 
		{
			int partitionIndex = partition(toBeSorted, low, high);
			quickSortByClientID(toBeSorted, low, partitionIndex-1);
			quickSortByClientID(toBeSorted, partitionIndex+1, high);
		}
	}
	
	public static int partition(ArrayList<Tuple> toBeSorted, int low, int high) {
        Tuple pivot = toBeSorted.get(high);
        int i = (low-1);

        for (int j = low; j < high; j++) 
        {
            if (toBeSorted.get(j).clientId <= pivot.clientId) 
            {
                i++;

                Tuple swapTemp = toBeSorted.get(i);
                toBeSorted.set(i, toBeSorted.get(j));
                toBeSorted.set(j, swapTemp);
            }
        }

        Tuple swapTemp = toBeSorted.get(i+1);
        toBeSorted.set(i+1, toBeSorted.get(high));
        toBeSorted.set(high, swapTemp);

        return i+1;
    }
	
//	public static void quickSortByClientID(Block toBeSorted, int low, int high) 
//	{
//		if(toBeSorted.getTuple(high) == null) 
//		{
//			return;
//		}
//		if (low < high) 
//		{
//			int partitionIndex = partition(toBeSorted, low, high);
//			quickSortByClientID(toBeSorted, low, partitionIndex-1);
//			quickSortByClientID(toBeSorted, partitionIndex+1, high);
//		}
//	}
//    
//	public static int partition(Block toBeSorted, int low, int high) {
//        Tuple pivot = toBeSorted.getTuple(high);
//        int i = (low-1);
//
//        for (int j = low; j < high; j++) 
//        {
//            if (toBeSorted.getTuple(j).clientId <= pivot.clientId) 
//            {
//                i++;
//
//                Tuple swapTemp = toBeSorted.getTuple(i);
//                toBeSorted.setTuple(i, toBeSorted.getTuple(j));
//                toBeSorted.setTuple(j, swapTemp);
//            }
//        }
//
//        Tuple swapTemp = toBeSorted.getTuple(i+1);
//        toBeSorted.setTuple(i+1, toBeSorted.getTuple(high));
//        toBeSorted.setTuple(high, swapTemp);
//
//        return i+1;
//    }
	
	

    public static void merge(int numFilesToRead) {
		final int blockSize = 1024;
		final int memoryLimit = 60;// 5M: 60, 10M:213
		System.out.println(Runtime.getRuntime().freeMemory());
		System.out.println(memoryLimit);
		int maxBlocksPerFile = Math.min(memoryLimit, numFilesToRead);
		int numPasses = (int) Math.ceil(Math.log(numFilesToRead)/Math.log(memoryLimit));

		for (int i = 0; i < numPasses; i++) {
			int readerIndex = 0;
			int writerIndex = 0;
			int numBlocksWrite = 0;
			ArrayList<Block> buffer = new ArrayList<>();
			Block output = new Block();

			Writer writer = new Writer(outputPath + inputFileName + "_pass_" + (i+1) + "_" + writerIndex + fileExtension);
			ArrayList<Reader> readers = new ArrayList<>();

			while (readerIndex < numFilesToRead){
				int k = 0;
				while (readerIndex < numFilesToRead && k<memoryLimit){
					Reader reader = new Reader(outputPath + inputFileName + "_pass_" + i + "_" + readerIndex + fileExtension);
					readers.add(reader);
					reader.readBlock();
					readerIndex++;
					buffer.add(reader.currentBlock);
					k++;
				}

				while (!buffer.isEmpty()){
					Block block = buffer.stream()
							.min(Comparator.comparingInt(b->b.getTuple(0).clientId))
							.get();
					Tuple tuple = block.getTuple(0);
					output.addTuple(tuple);

					if (output.isFull()) {
						if (numBlocksWrite >= maxBlocksPerFile) {
							writerIndex++;
							writer = new Writer(outputPath + inputFileName + "_pass_" + (i+1) + "_" + writerIndex + fileExtension);
							numBlocksWrite = 0;
						}
						writer.write(output);
						numBlocksWrite++;
						output = new Block();
					}
					block.removeTuple(0);

					if (block.records.isEmpty()){
						int bufferIndex = buffer.indexOf(block);
						Reader reader = readers.get(bufferIndex);
						reader.readBlock();
						if (reader.finishedReading && reader.currentBlock.records.isEmpty()){
							readers.remove(bufferIndex);
							buffer.remove(bufferIndex);
							continue;
						}
						buffer.set(bufferIndex, reader.currentBlock);
					}

				}

				if (!output.records.isEmpty()) {
					if (numBlocksWrite >= maxBlocksPerFile) {
						writerIndex++;
						writer = new Writer(outputPath + inputFileName + "_pass_" + (i+1) + "_" + writerIndex + fileExtension);
					}
					writer.write(output);
				}
			}

			numFilesToRead = writerIndex + 1;
			maxBlocksPerFile *= memoryLimit;
			System.out.println("======Pass " + i + " Finished======");
		}
	}
    
    
    
    
    
    
}
