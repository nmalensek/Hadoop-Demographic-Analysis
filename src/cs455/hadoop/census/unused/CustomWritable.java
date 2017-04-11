package cs455.hadoop.census.unused;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CustomWritable implements Writable {

    //must define strings with initial 0 values to avoid number format and null pointer exceptions
    //because not all mapped files will contain question data
    private String questionOne = "0:0";
    private String questionTwo = "0:0:0";
    private String questionThree = "0:0:0:0:0:0:0";
    private String questionFour = "0:0";
    private String questionFiveTotalHomes = "0";
    private String questionFiveHomeValues = "0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0";
    private String questionSix;
    private String questionSeven;
    private String questionEight;
    private String questionNine;

    public CustomWritable() {}

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        questionOne = WritableUtils.readString(dataInput);
        questionTwo = WritableUtils.readString(dataInput);
        questionThree = WritableUtils.readString(dataInput);
        questionFour = WritableUtils.readString(dataInput);
        questionFiveTotalHomes = WritableUtils.readString(dataInput);
        questionFiveHomeValues = WritableUtils.readString(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, questionOne);
        WritableUtils.writeString(dataOutput, questionTwo);
        WritableUtils.writeString(dataOutput, questionThree);
        WritableUtils.writeString(dataOutput, questionFour);
        WritableUtils.writeString(dataOutput, questionFiveTotalHomes);
        WritableUtils.writeString(dataOutput, questionFiveHomeValues);
    }

    public String getQuestionOne() {return questionOne;}
    public void setQuestionOne(String questionOne) {this.questionOne = questionOne;}

    public String getQuestionTwo() {return questionTwo;}
    public void setQuestionTwo(String questionTwo) {this.questionTwo = questionTwo;}

    public String getQuestionThree() {return questionThree;}
    public void setQuestionThree(String questionThree) {this.questionThree = questionThree;}

    public String getQuestionFour() {return questionFour;}
    public void setQuestionFour(String questionFour) {this.questionFour = questionFour;}

    public String getQuestionFiveTotalHomes() {return questionFiveTotalHomes;}
    public void setQuestionFiveTotalHomes(String questionFiveTotalHomes) {this.questionFiveTotalHomes = questionFiveTotalHomes;}

    public String getQuestionFiveHomeValues() {return questionFiveHomeValues;}
    public void setQuestionFiveHomeValues(String questionFiveHomeValues) {this.questionFiveHomeValues = questionFiveHomeValues;}
}
