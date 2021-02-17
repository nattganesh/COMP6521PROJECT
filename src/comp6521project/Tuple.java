package comp6521project;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Tuple {
     int clientId;
     String name;
     int gender;
     int ssn;
     String itemOrdered;
     int quantityOrdered;
     Date orderDate;

    public Tuple(int clientId, String name, int gender, int ssn,
                 String itemOrdered, int quantityOrdered, Date orderDate) {
        this.clientId = clientId;
        this.name = name;
        this.gender = gender;
        this.ssn = ssn;
        this.itemOrdered = itemOrdered;
        this.quantityOrdered = quantityOrdered;
        this.orderDate = orderDate;
    }

    public Tuple(String input){
        // TODO convert input string into the data structure
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date date = format.parse(input);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString(){
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        return String.format("%1$8d", clientId)+
                String.format("%1$-30s", name)+
                String.format("%1$-1d", gender)+
                String.format("%1$-9d", ssn)+
                String.format("%1$-45s", itemOrdered)+
                String.format("%1$-7d", quantityOrdered)+
                formatter.format(orderDate);
    }
}
