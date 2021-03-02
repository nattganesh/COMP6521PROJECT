package comp6521project;

import java.util.ArrayList;

public class Main {

//	public static String inputFileName = "HalfMillionData";
	public static String inputFileName = "OneMillionData";

//	public static String inputFileName2 = "HalfMillionData2";
	public static String inputFileName2 = "OneMillionData2";

	public static String outputFileName = "Output";
	public static String fileExtension = ".txt";
    public static String inputPath = System.getProperty("user.dir")+"/textfiles/input/";
    public static String outputPath = System.getProperty("user.dir")+"/textfiles/output/";
    
    static int totalIO = 0;
    
	public static void main(String[] args) {
		
		long sortStart = System.nanoTime();
		
		System.out.println("-------------Currently Reading and Sorting -- Pass 1 --------------------");
		Reader r = new Reader(inputPath + inputFileName + fileExtension);

		//Number of blocks we can process based of available memory
		int maxNumberOfBlocksToProcess = Math.floorDiv((int) (Runtime.getRuntime().maxMemory()*0.85f), Block.bytesPerBlock);
//		System.out.println("maxNumberOfBlocksToProcess: " + Runtime.getRuntime().maxMemory()*0.85f/1048576);
		
		int[] numFilesandSortIO = readAndSort(r, 0, 0);
		if(inputFileName != inputFileName2) 
		{
			System.gc();
//			System.out.println("numFilesToRead: " + numFiles);
			inputFileName = inputFileName2; // To indicate we have acknowledge both files
			// If a second file exists
			r = new Reader(inputPath + inputFileName2 + fileExtension);
			numFilesandSortIO = readAndSort(r, numFilesandSortIO[0], numFilesandSortIO[1]);
		}
		r = null;
		long sortEnd = System.nanoTime();
		System.gc();
		totalIO += numFilesandSortIO[1];
		System.out.println("Disk I/O at Read and Sort phase (PASS 1): " + numFilesandSortIO[1]);
		System.out.println("Read and Sort phase (PASS 1) Execution Time: " + (sortEnd-sortStart)/1_000_000_000 + "s");

		System.out.println("Total number of records in the resulting tables T1 and T2: " + Reader.totalNumberOfTuples);

		long mergeStart = System.nanoTime();
		
		String mergedFile = merge(numFilesandSortIO[0], maxNumberOfBlocksToProcess, Reader.totalNumberOfTuples);
		long mergeEnd = System.nanoTime();
		System.out.println("Merge Phase (PASS 2) Execution Time: " + (mergeEnd-mergeStart)/1_000_000_000 + "s");
		System.gc();
		long processStart = System.nanoTime();
		processTuples(mergedFile);
		long processEnd = System.nanoTime();
		
		System.out.println("Process Data Execution Time: " + (processEnd-processStart)/1_000_000_000 + "s");

		System.out.println("Total number of disk I/Os performed to produce T: " + totalIO);
		System.out.println("Total execution time to produce T: " + (processEnd-sortStart)/1_000_000_000 + "s");
//		System.out.println("Complete");
//		System.out.println("Total number of Disk IO's in the resulting table T should be: " + numFilesandSortIO[1] * 3);
	}

	public static int[] readAndSort(Reader r, int numFiles, int sortIO) 
	{
		int countFiles = numFiles;
		while(!r.finishedReading)
		{
				System.gc();
				int countNumberOfBlocksRead = r.readBlocks(new ArrayList<>());
				sortIO += countNumberOfBlocksRead; // reading file

				if(!r.currentTuples.isEmpty()) {

	//			System.out.println("Read " + countNumberOfBlocksRead + " Blocks.");
				quickSortByClientID(r.currentTuples, 0, r.currentTuples.size() - 1);

				int numRecordsInBlock = 0;
				Block b = new Block();
				ArrayList<Block> currentBlocks = new ArrayList<>();
				for(Tuple t: r.currentTuples)
				{
					if(numRecordsInBlock < Block.recordsPerBlock)
					{
						b.addTuple(t);
						numRecordsInBlock++;
					}
					else
					{
						currentBlocks.add(b);
						numRecordsInBlock = 0;
						b = new Block();
						b.addTuple(t);
						numRecordsInBlock++;
					}
					if(t == r.currentTuples.get(r.currentTuples.size() - 1))
					{
						currentBlocks.add(b);
					}
				}

				if(!currentBlocks.isEmpty())
				{
					Writer writer = new Writer(outputPath + outputFileName + "_pass_0_" + countFiles + fileExtension);
					writer.writeChunk(currentBlocks);
					sortIO += currentBlocks.size(); // writing to file
					countFiles++;
				}
			}
		}
		
//		if(inputFileName != inputFileName2) 
//		{
////			System.out.println("numFilesToRead: " + numFiles);
//			inputFileName = inputFileName2; // To indicate we have acknowledge both files
//			// If a second file exists
//			r = new Reader(inputPath + inputFileName2 + fileExtension);
//			readAndSort(r, countFiles, sortIO);
//		}
//		else 
//		{
//			System.out.println("Disk I/O at sort phase: " + sortIO);
//			totalIO += sortIO;
//		}
//		numFilesCounts+= numFiles;
//		System.out.println("numFilesToRead: " + numFilesCounts);
		int[] ret = {countFiles, sortIO};
		return ret;
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
	
    public static String merge(int numFilesToRead, int maxBlocksToProcess, int numTuplesToProcess) {
		System.out.println("-------------Currently starting Merge -- Pass 2 --------------------");
		int numIO = 0;
		int memoryLimit = (int)(Runtime.getRuntime().freeMemory())/Block.bytesPerBlock;// (int)(maxBlocksToProcess/4.5);
		int numTuplesWrote = 0;
		int readIO = 0;
		int numPasses = 0;
		int writerIndex = Integer.MAX_VALUE;

		while (writerIndex > 1){
			int readerIndex = 0;
			writerIndex = 0;
			Block output = new Block();

			while (readerIndex < numFilesToRead){
				ArrayList<Reader> readers = new ArrayList<>();
				Writer writer = new Writer(outputPath + outputFileName + "_pass_" + (numPasses+1) + "_" + writerIndex + fileExtension);

				int numBlocksToRead = Math.min(memoryLimit, maxBlocksToProcess);
				int k = 0;

				while (readerIndex < numFilesToRead && k < numBlocksToRead && Runtime.getRuntime().freeMemory() > (Runtime.getRuntime().maxMemory()*0.15f)){
					Reader reader = new Reader(outputPath + outputFileName + "_pass_" + numPasses + "_" + readerIndex + fileExtension);
					readers.add(reader);
					reader.readBlock();
					reader.resetCurrentTuples();
					numIO++;
					readIO++;
					readerIndex++;
					k++;
				}

				while (!readers.isEmpty()){
					Reader reader = getMin(readers);
					Tuple tuple = reader.currentBlock.getTuple(0);

					if (output.isFull()) {
						writer.write(output);
						numTuplesWrote += output.size();
						numIO++;
						output = new Block();
						output.addTuple(tuple);
					}
					else
					{
						output.addTuple(tuple);
					}
					reader.currentBlock.records.remove(0);

					if (reader.currentBlock.records.isEmpty()){
						reader.readBlock();
						reader.resetCurrentTuples();
						readIO++;
						if(!reader.currentBlock.records.isEmpty()) {
							numIO++;
						}
						if (reader.finishedReading && reader.currentBlock.records.isEmpty()){
							readIO--;
							readers.remove(reader);
						}
					}
				}

				if (!output.records.isEmpty()) {
					writer.write(output);
					numTuplesWrote += output.size();
					numIO++;
				}
				writerIndex++;
			}
			System.out.println("Pass " + numPasses + " Finished");
			numFilesToRead = writerIndex;
			numPasses++;
		}
		System.out.println("ReadIO: "+readIO);
//		System.out.println("Total number of records in the resulting tables T1 and T2: "+numTuplesWrote/numPasses);
		System.out.println("Disk I/O at Merge phase (PASS 1): " + (numIO/numPasses));
		totalIO += numIO;
		return outputPath + outputFileName + "_pass_" + numPasses + "_0" + fileExtension;
	}

	public static Reader getMin(ArrayList<Reader> readers) {
		Reader min = null;
		for (Reader reader: readers) {
			if (min == null || reader.currentBlock.records.get(0).clientId < min.currentBlock.records.get(0).clientId) {
				min = reader;
			}
		}
		return min;
	}
    
    public static void processTuples(String sortedFile) {
    	
    	System.out.println("------------- Currently Doing The Requested Processing --------------------");
    	
		Writer writer = new Writer(outputPath + outputFileName + "_processed" + fileExtension);
		Reader reader = new Reader(sortedFile);

		int countRecordsInT1andT2 = 0;
		int countRecordsInT = 0;
		int countBlocksInT = 0;
		int processIO = 0;

		Block output = new Block();
		ArrayList<Tuple> tuples = new ArrayList<>();
		int clientId = -1;

		while (!reader.finishedReading) {
			int countNumberOfBlocksRead = reader.readBlocks(new ArrayList<>());
			processIO += countNumberOfBlocksRead;

			for (Tuple tuple: reader.currentTuples)
			{
				countRecordsInT1andT2++;
				if (clientId != -1 && clientId != tuple.clientId) 
				{
					output.addTuple(new ProcessedTuple(tuples));
					tuples = new ArrayList<>();
				}
				if (output.isFull()) 
				{
					countRecordsInT += output.size();
					writer.write(output);
					output = new Block();
					countBlocksInT++;
					processIO++;
				}				
				tuples.add(tuple);
				clientId = tuple.clientId;
			}
		}
		
		if (!output.isEmpty()) 
		{
			countRecordsInT += output.size();
			writer.write(output);
			countBlocksInT++;
			processIO++;
		}
		System.out.println("Total number of records in the resulting tables T1 and T2: " + countRecordsInT1andT2);
		System.out.println("Total number of IO's in process phase: " + processIO); 
		System.out.println("================================================================"); 
		System.out.println("Total number of records in the resulting table T: " + countRecordsInT);
		System.out.println("Total number of blocks in the resulting table T: " + countBlocksInT); 
		
		totalIO += processIO;
//		System.out.println("Total number of disk I/Os performed to produce T: " + countBlocksInT);
//		System.out.println("Total execution time to produce T: " + countBlocksInT);
	}
}