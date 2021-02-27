package comp6521project;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class ProcessedTuple extends Tuple{
    private int numOrders;

    public ProcessedTuple(ArrayList<Tuple> tuples) {
        super();
        int totalQuantityOrdered = 0;
        Date lastDate = null;
        for (Tuple tuple:tuples) {
            totalQuantityOrdered += tuple.quantityOrdered;
            if (lastDate == null || lastDate.compareTo(tuple.orderDate) < 0) {
                lastDate = tuple.orderDate;
            }
        }
        Tuple tuple = tuples.get(0);
        this.clientId = tuple.clientId;
        this.name = tuple.name;
        this.gender = tuple.gender;
        this.ssn = tuple.ssn;
        this.itemOrdered = tuple.itemOrdered;
        this.quantityOrdered = totalQuantityOrdered;
        this.orderDate = lastDate;
        this.numOrders = tuples.size();
    }

    @Override
    public String toString() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        return String.format("%1$8d", clientId)+
                String.format("%1$-30s", name)+
                String.format("%1$-1d", gender)+
                String.format("%1$-9d", ssn)+
                String.format("%1$-45s", itemOrdered)+
                String.format("%1$08d", quantityOrdered)+
                formatter.format(orderDate)+
                String.format("%1$02d", numOrders);
    }
}
