package comp6521project;

import java.util.ArrayList;
import java.util.Comparator;

public class Main {

	public static final int kiloByte = 1024;
	public static final int megaByte = 1048576;
//	public static final int maxMemory = kiloByte * 555;
	public static final int maxMemory = 5 * megaByte; //5Mb
//	public static final int maxMemory = 10 * megaByte; //Case 1 : 10Mb
//	public static final int maxMemory = 20 * megaByte; //Case 2 : 20Mb
	
//	public static String inputFileName = "5K";
//	public static String inputFileName = "Input_Example";
	public static String inputFileName = "HalfMillionData";
//	public static String inputFileName = "HalfMillionData2";
//	public static String inputFileName = "OneMillionData";
//	public static String inputFileName = "OneMillionData2";
//	public static String inputFileName = "Input_300000_records";
//	public static String inputFileName = "mergeCheck";
//	public static String inputFileName = "sortCheck";
	
//	public static String inputFileName2 = "50K";
//	public static String inputFileName2 = "Input_Example";
//	public static String inputFileName2 = "sortCheck2";
//	public static String inputFileName2 = "HalfMillionData";
	public static String inputFileName2 = "HalfMillionData2";
//	public static String inputFileName2 = "OneMillionData";
//	public static String inputFileName2 = "OneMillionData2";
//	public static String inputFileName2 = "Input_100000_records";

	public static String outputFileName = "Output";
	public static String fileExtension = ".txt";
    public static String inputPath = System.getProperty("user.dir")+"/textfiles/input/";
    public static String outputPath = System.getProperty("user.dir")+"/textfiles/output/";
    
	public static void main(String[] args) {

		System.out.println("Currently Reading and Sorting -- Phase 1 ");
		Reader r = new Reader(inputPath + inputFileName + fileExtension);

		//Number of blocks we can process based of available memory
		int maxNumberOfBlocksToProcess = Math.floorDiv(maxMemory, Block.bytesPerBlock);

		long sortStart = System.nanoTime();
		readAndSort(r, 0, 0);
		r = null;
		long sortEnd = System.nanoTime();
		System.out.println("Sort Phase Execution Time: " + (sortEnd-sortStart)/1_000_000_000 + "s");

		System.out.println("Number of Tuples " + Reader.totalNumberOfTuples);

		long mergeStart = System.nanoTime();
		String mergedFile = merge(numFilesCounts - 1, maxNumberOfBlocksToProcess);
		long mergeEnd = System.nanoTime();
		System.out.println("Merge Phase Execution Time: " + (mergeEnd-mergeStart)/1_000_000_000 + "s");
		
		processTuples(mergedFile, maxNumberOfBlocksToProcess);
//		processTuples(outputPath + outputFileName + "_pass_" + 1 + "_" + 0 + fileExtension, maxNumberOfBlocksToProcess);

		System.out.println("Complete");
	}
	static int numFilesCounts = 0;
	public static int readAndSort(Reader r, int numFiles, int sortIO) 
	{
		while(!r.finishedReading)
		{
			
			int countNumberOfBlocksRead = r.readBlocks(new ArrayList<>());
			sortIO += countNumberOfBlocksRead; // reading file
			
			if(!r.currentTuples.isEmpty()) {
			
//			System.out.println("Read " + countNumberOfBlocksRead + " Blocks.");
			quickSortByClientID(r.currentTuples, 0, r.currentTuples.size() - 1);
			
			Writer writer = new Writer(outputPath + outputFileName + "_pass_0_" + numFiles + fileExtension);
			
			int numRecordsInBlock = 0;
			Block b = new Block();
			ArrayList<Block> currentBlocks = new ArrayList<>();
			for(Tuple t: r.currentTuples) 
			{
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
			}
			
			if(!currentBlocks.isEmpty()) 
			{
				writer.writeChunk(currentBlocks);
				sortIO += currentBlocks.size(); // writing to file
				numFiles++;
			}
			}
		}
		
		if(inputFileName != inputFileName2) 
		{
//			System.out.println("numFilesToRead: " + numFiles);
			inputFileName = inputFileName2; // To indicate we have acknowledge both files
			// If a second file exists
			r = new Reader(inputPath + inputFileName2 + fileExtension);
			readAndSort(r, numFiles, sortIO);
		}
		else 
		{
			System.out.println("Disk I/O at sort phase: " + sortIO);
		}
		numFilesCounts+= numFiles;
//		System.out.println("numFilesToRead: " + numFilesCounts);
		return numFiles;
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
	

    public static String merge(int numFilesToRead, int numBlocksPerFile) {
    	System.out.println("numFilesToRead: " + numFilesToRead);
		System.out.println("Merge start");
		int numIO = 0;
		int memoryLimit = (int)(numBlocksPerFile/4.5);
		int maxBlocksPerFile = numBlocksPerFile * memoryLimit;
		int numPasses = (int) Math.ceil(Math.log(numFilesToRead)/Math.log(memoryLimit));
		int writerIndex = 0;

		for (int i = 0; i < numPasses; i++) {
			int readerIndex = 0;
			int numBlocksWrote = 0;
			ArrayList<Block> buffer = new ArrayList<>();
			Block output = new Block();

			Writer writer = new Writer(outputPath + outputFileName + "_pass_" + (i+1) + "_" + writerIndex + fileExtension);
			ArrayList<Reader> readers = new ArrayList<>();

			while (readerIndex < numFilesToRead){
				int numBlocksToRead = Math.min(memoryLimit, numBlocksPerFile);
				int k = 0;
				while (readerIndex < numFilesToRead && k < numBlocksToRead && Runtime.getRuntime().freeMemory() > (Runtime.getRuntime().maxMemory()*0.1f)){
//						System.out.println("Here " + Runtime.getRuntime().freeMemory()/1024);
					Reader reader = new Reader(outputPath + outputFileName + "_pass_" + i + "_" + readerIndex + fileExtension);
					readers.add(reader);
					reader.readBlock();
					reader.resetCurrentTuples();
					numIO++;
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
						if (numBlocksWrote >= maxBlocksPerFile) {
							writerIndex++;
//							writer = new Writer(outputPath + outputFileName + "_pass_" + (i+1) + "_" + writerIndex + fileExtension);
							numBlocksWrote = 0;
						}
						writer.write(output);
						numIO++;
						numBlocksWrote++;
						output = new Block();
					}
					block.records.remove(0);
					System.gc();
					
					if (block.records.isEmpty()){
						int blockIndex = buffer.indexOf(block);
						Reader reader = readers.get(blockIndex);
						reader.readBlock();
						reader.resetCurrentTuples();
						k++;
						numIO++;
						if (reader.finishedReading && reader.currentBlock.records.isEmpty()){
							readers.remove(blockIndex);
							buffer.remove(blockIndex);
							continue;
						}
						buffer.set(blockIndex, reader.currentBlock);
					}
				}
				System.out.println("Processed " + numFilesToRead + " files");

				if (!output.records.isEmpty()) {
					if (numBlocksWrote >= maxBlocksPerFile) {
						writerIndex++;
//						writer = new Writer(outputPath + outputFileName + "_pass_" + (i+1) + "_" + writerIndex + fileExtension);
					}
					writer.write(output);
					numIO++;
				}

			}
			writerIndex = 0;
			numFilesToRead = writerIndex + 1;
			maxBlocksPerFile *= memoryLimit;
			System.out.println("Pass " + i + " Finished");
		}
		System.out.println("Disk I/O at merge phase: " + numIO);
		return outputPath + outputFileName + "_pass_" + numPasses + "_" + writerIndex + fileExtension;
	}
    
    public static void processTuples(String sortedFile, int maxBlocksToRead) {
		Writer writer = new Writer(outputPath + outputFileName + "_processed" + fileExtension);
		Reader reader = new Reader(sortedFile);

		int countRecordsInT = 0;
		int countBlocksInT = 0;
		Block output = new Block();
		
		while (!reader.finishedReading) {
			reader.readBlocks(new ArrayList<>());
			output = new Block();

			int clientId = -1;
			ArrayList<Tuple> tuples = new ArrayList<>();

//			System.out.println("Number of Tuples " + reader.currentTuples.size());
			for (Tuple tuple: reader.currentTuples) {
				countRecordsInT++;
				if (clientId != -1 && clientId != tuple.clientId) 
				{
					output.addTuple(new ProcessedTuple(tuples));
					tuples = new ArrayList<>();
				}
					if (output.isFull()) { // || tuple == reader.currentTuples.get(reader.currentTuples.size() - 1)
						writer.write(output);
						output = new Block();
						countBlocksInT++;
					}
				
				tuples.add(tuple);
				clientId = tuple.clientId;
			}
		}
		
		if (!output.isEmpty()) 
		{
			writer.write(output);
			countBlocksInT++;
		}
		System.out.println("Total number of records in the resulting table T: " + countRecordsInT);
		System.out.println("Total number of blocks in the resulting table T: " + countBlocksInT); //Math.ceil(countRecordsInT / Block.bytesPerBlock)
	}




}