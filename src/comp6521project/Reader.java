package comp6521project;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * 
 * @author Natheepan
 *
 */
public class Reader{
	
	public BufferedReader reader;
	public Block currentBlock;
	
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
				if (nextLine == null || nextLine.trim().equals("")) 
				{
	                break;
	            }
				block.addTuple(new Tuple(nextLine));
				i++;
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			}
		}
		currentBlock = block;
	}
}
