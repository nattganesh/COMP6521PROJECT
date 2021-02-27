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

     public Tuple() {}

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

    public Tuple(String input) {
        this.clientId = Integer.parseInt(input.substring(0,8));// INT(8)
        this.name = input.substring(8,38);// CHAR(30)
        this.gender = Integer.parseInt(input.substring(38,39));// INT(1)
        this.ssn = Integer.parseInt(input.substring(39,48));// INT(9)
        this.itemOrdered = input.substring(48,93);// CHAR(45)
        this.quantityOrdered = Integer.parseInt(input.substring(93,100));// INT(7)
        try {
            this.orderDate = new SimpleDateFormat("yyyy-MM-dd").parse(input.substring(100,110));// CHAR(10)
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
