package comp6521project;

import java.util.ArrayList;

public class Block {
    public static final int recordsPerBlock = 9;
    public static final int bytesPerBlock = 1024;
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
