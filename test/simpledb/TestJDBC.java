package simpledb;

import org.junit.Before;
import org.junit.Test;
import simpledb.jdbc.SDBConnection;
import simpledb.jdbc.Driver;
import simpledb.jdbc.SDBStatement;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TestJDBC{
    private Connection conn=null;

    @Before public void init(){
        try{
            Parser p= new Parser();
            String []argv={"catalog.txt"};
            p.start(argv);
            String url= "jdbc.Driver";
            Driver driver= new Driver();
            conn = (Connection) driver.connect("",null);
        }
        catch(SQLException s){
            s.printStackTrace();
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }

    @Test public void simpledbtest() {
        try{
            Statement s=conn.createStatement();
            //s.addBatch();
            s.execute("select * from data;");
            s.execute("select * from data2;");
            s.execute("select * from data,data2 where data.f1<10 and data.f1=data2.f1 and data2.f4=123;");
        }
        catch(SQLException s){
            s.printStackTrace();
        }
    }

}
