package comp6521project;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * 
 * @author Xiyue Li
 *
 */
public class Writer {
    BufferedWriter writer;
    public Writer(String file){
        try{
            this.writer = new BufferedWriter(new FileWriter(file));
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public void write(Block block){
        try {
            writer.write(block.toString());
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close(){
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
