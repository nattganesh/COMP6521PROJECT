package comp6521project;

public class Block {
    public static final int recordsPerBlock = 9;
    Tuple[] records;

    public Block(){
        records = new Tuple[recordsPerBlock];
    }

    public void addTuple(Tuple tuple) {
        for (int i=0;i<recordsPerBlock;i++) {
            if (records[i] == null)
            {
                records[i] = tuple;
                return;
            }
        }
    }
    
    public Tuple getTuple(int index) 
    {
    	if (index > -1 && index < recordsPerBlock)
    	{
    		return records[index];
    	}
    	return null;
    }
    
    public void setTuple(int index, Tuple tuple) 
    {
    	if (index > -1 && index < recordsPerBlock)
    	{
    		records[index] = tuple;
    	}
    }

    @Override
    public String toString(){
        String result = "";
        for (int i=0;i<recordsPerBlock;i++){
            if (records[i]!=null){
                result+=records[i].toString();
                result+="\n";
            }
        }
        return result;
    }
}
