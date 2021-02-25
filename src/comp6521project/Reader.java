package comp6521project;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * 
 * @author Natheepan
 *
 */
public class Reader{
	
	public BufferedReader reader;
//	public ArrayList<Block> currentBlocks = new ArrayList<>();
	public ArrayList<Tuple> currentTuples = new ArrayList<>();
	public boolean finishedReading = false;
	public Block currentBlock;
	
	static int totalNumberOfTuples = 0;
	
	public Reader(String fileName) {
		try{
            this.reader = new BufferedReader(new FileReader(fileName));
        }catch (IOException e){
            e.printStackTrace();
        }
	}
	
	public void readBlock() {
		Block block = new Block();
		int i = 0;
		
		while(i < Block.recordsPerBlock) 
		{
			try 
			{
				String nextLine = reader.readLine();
				if (nextLine == null || nextLine.trim().equals("") || nextLine == "\n" || nextLine == "\r")
				{
					finishedReading = true;
	                break;
	            }
				totalNumberOfTuples++;
				Tuple temp = new Tuple(nextLine);
				block.addTuple(temp);
				currentTuples.add(temp);
				i++;
//				System.out.println(nextLine);
			} 
			catch (IOException e) 
			{
				finishedReading = true;
				e.printStackTrace();
			}
		}
		currentBlock = block;
//		currentBlocks.add(block);
	}
	
	public int readBlocks(int numBlocks) 
	{
		return readBlocks(numBlocks, new ArrayList<Tuple>());
	}
	
	public int readBlocks(int numBlocks, ArrayList<Tuple> tuples) 
	{
		this.currentTuples = tuples;
		int countNumberOfBlocksRead = 0;
		while(numBlocks > 0) 
		{
			countNumberOfBlocksRead++;
			numBlocks--;
			
			readBlock();
			
			if(finishedReading)
				break;
		}
		
//		System.out.println("Read " + countNumberOfBlocksRead + " Blocks.");
		return countNumberOfBlocksRead;
	}
}
