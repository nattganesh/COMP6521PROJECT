package comp6521project;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * 
 * @author Xiyue Li
 *
 */
public class Writer {
    String file;
    BufferedWriter writer;
    public Writer(String file){
        try{
            this.file = file;
            this.writer = new BufferedWriter(new FileWriter(file));
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public void write(Block block){
        try {
            writer = new BufferedWriter(new FileWriter(file, true));
            writer.write(block.toString());
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void writeChunk(ArrayList<Block> blocks){
        try {
        	for(Block b : blocks) 
        	{
        		writer.write(b.toString());
                writer.flush();
        	}
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
