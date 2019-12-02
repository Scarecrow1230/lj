package com.qf;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Bean implements WritableComparable<Bean> {
    private String move;
    private String sorce;
    private String date;
    private String user;


    public Bean() {
    }

    public Bean(String move, String sorce, String date, String user) {
        this.move = move;
        this.sorce = sorce;
        this.date = date;
        this.user = user;
    }



    @Override
    public int compareTo(Bean o) {
        int result=this.user.compareTo(o.user);
        if (result==0){
            return  -this.sorce.compareTo(o.sorce);
        }
        return  result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(move);
        dataOutput.writeUTF(sorce);
        dataOutput.writeUTF(date);
        dataOutput.writeUTF(user);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        move=dataInput.readUTF();
        sorce=dataInput.readUTF();
        date=dataInput.readUTF();
        user=dataInput.readUTF();

    }

    @Override
    public String toString() {
        return  user+" "+sorce;

    }
}
