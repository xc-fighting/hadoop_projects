

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBOutputWritable implements DBWritable{
    private String start_word="";
    private String follow_word="";
    private int count=0;
    public DBOutputWritable(String s,String f,int c){
        this.start_word=s;
        this.follow_word=f;
        this.count=c;
    }
    public void readFields(ResultSet result) throws SQLException{
          this.start_word=result.getString("starting_phrase");
          this.follow_word=result.getString("following_word");
          this.count=result.getInt("count");
    }
    public void write(PreparedStatement statement) throws SQLException{
          statement.setString(1,this.start_word);
          statement.setString(2,this.follow_word);
          statement.setInt(3,this.count);
    }
}
