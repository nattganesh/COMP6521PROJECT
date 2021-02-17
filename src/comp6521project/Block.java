package comp6521project;

public class Block {
    static final int recordsPerBlock = 9;
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

    @Override
    public String toString(){
        String result = "";
        for (int i=0;i<recordsPerBlock;i++){
            if (records[i]!=null){
                result+=records[i].toString();
            }
        }
        return result;
    }
}
