package comp6521project;

import java.util.ArrayList;

public class Block {
    public static final int recordsPerBlock = 9;
    public static final int bytesPerBlock = 2 * Main.kiloByte;// 1024;
    /**
     * https://www.javamex.com/tutorials/memory/string_memory_usage.shtml
     * the minimum memory usage of a Java String
     * multiply the number of characters of the String by two;
     * add 38;
     * if the result is not a multiple of 8, round up to the next multiple of 8;
     * the result is generally the minimum number of bytes taken up on the heap by the String.
//          byte[] utf16Bytes;
//    		try {
//    			utf16Bytes = result.getBytes("UTF-16BE");
//    			System.out.println(utf16Bytes.length);
//    		} catch (UnsupportedEncodingException e) {
//    			// TODO Auto-generated catch block
//    			e.printStackTrace();
//    		}
     * public static final int bytesPerBlock = 2 * Main.kiloByte;
     */
    
    ArrayList<Tuple> records;

    public Block(){
        records = new ArrayList<>();
    }

    public void addTuple(Tuple tuple) {
        for (int i=0;i<recordsPerBlock;i++) {
                records.add(tuple);
                return;
        }
    }

    public void removeTuple(int index) {
        records.remove(index);
    }
    
    public Tuple getTuple(int index) 
    {
    	if (index > -1 && index < recordsPerBlock)
    	{
    		return records.get(index);
    	}
    	return null;
    }
    
    public void setTuple(int index, Tuple tuple) 
    {
    	if (index > -1 && index < recordsPerBlock)
    	{
    		records.set(index, tuple);
    	}
    }

    @Override
    public String toString(){
        String result = "";
        for (int i=0;i<records.size();i++){
            result+=records.get(i).toString() + "\n";
        }
        return result;
    }

    public boolean isFull() { return records.size() >= recordsPerBlock; }
}
