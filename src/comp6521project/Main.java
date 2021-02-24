package comp6521project;

import java.util.ArrayList;
import java.util.Comparator;

public class Main {

//	public static String inputFileName = "Input_Example";
	public static String inputFileName = "mergeCheck";
//	public static String inputFileName = "sortCheck";
	public static String fileExtension = ".txt";
    public static String inputPath = System.getProperty("user.dir")+"/textfiles/input/";
    public static String outputPath = System.getProperty("user.dir")+"/textfiles/output/";
    
    
    
	public static void main(String[] args) {
		System.out.println(Runtime.getRuntime().maxMemory());
		
		Reader r = new Reader(inputPath + inputFileName + fileExtension);
		int numBlocks = 0;

		while(!r.finishedReading)
		{
			r.readBlock();

			quickSortByClientID(r.currentBlock, 0, r.currentBlock.records.size() - 1);

			Writer writer = new Writer(outputPath + inputFileName + "_pass_0_" + numBlocks + fileExtension);
			writer.write(r.currentBlock);
			writer.close();
			numBlocks++;
			
//			System.out.println("\nBlock ID = " + numBlocks + "\n");
		}
		merge(numBlocks);

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
