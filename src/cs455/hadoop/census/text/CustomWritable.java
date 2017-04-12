package cs455.hadoop.census.text;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomWritable implements Writable {

    //must define strings with initial 0 values to avoid number format and null pointer exceptions
    //because not all mapped files will contain question data
    private String questionOne = "0:0";
    private String questionTwo = "0:0:0";
    private String questionThree = "0:0:0:0:0:0:0";
    private String questionFour = "0:0";
    private String questionFiveTotalHomes = "0";
    private String questionFiveHomeValues = "0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0";
    private String questionSixTotalRenters = "0";
    private String questionSixRenterValues = "0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0:0";
    private String questionSevenDwellingsWithRooms = "0";
    private String questionSevenRoomsPerHouse = "0:0:0:0:0:0:0:0:0";
    private String questionSevenAverageRooms = "0";
    private String questionEight = "0:0";
    private String questionNine = "0:0:0:0:0:0:0:0";

    public CustomWritable() {}

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        questionOne = WritableUtils.readString(dataInput);
        questionTwo = WritableUtils.readString(dataInput);
        questionThree = WritableUtils.readString(dataInput);
        questionFour = WritableUtils.readString(dataInput);
        questionFiveTotalHomes = WritableUtils.readString(dataInput);
        questionFiveHomeValues = WritableUtils.readString(dataInput);
        questionSixTotalRenters = WritableUtils.readString(dataInput);
        questionSixRenterValues = WritableUtils.readString(dataInput);
        questionSevenDwellingsWithRooms = WritableUtils.readString(dataInput);
        questionSevenRoomsPerHouse = WritableUtils.readString(dataInput);
        questionSevenAverageRooms = WritableUtils.readString(dataInput);
        questionEight = WritableUtils.readString(dataInput);
        questionNine = WritableUtils.readString(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, questionOne);
        WritableUtils.writeString(dataOutput, questionTwo);
        WritableUtils.writeString(dataOutput, questionThree);
        WritableUtils.writeString(dataOutput, questionFour);
        WritableUtils.writeString(dataOutput, questionFiveTotalHomes);
        WritableUtils.writeString(dataOutput, questionFiveHomeValues);
        WritableUtils.writeString(dataOutput, questionSixTotalRenters);
        WritableUtils.writeString(dataOutput, questionSixRenterValues);
        WritableUtils.writeString(dataOutput, questionSevenDwellingsWithRooms);
        WritableUtils.writeString(dataOutput, questionSevenRoomsPerHouse);
        WritableUtils.writeString(dataOutput, questionSevenAverageRooms);
        WritableUtils.writeString(dataOutput, questionEight);
        WritableUtils.writeString(dataOutput, questionNine);
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

    public String getQuestionSixTotalRenters() {return questionSixTotalRenters;}
    public void setQuestionSixTotalRenters(String questionSixTotalRenters) {this.questionSixTotalRenters = questionSixTotalRenters;}
    public String getQuestionSixRenterValues() {return questionSixRenterValues;}
    public void setQuestionSixRenterValues(String questionSixRenterValues) {this.questionSixRenterValues = questionSixRenterValues;}

    public String getQuestionSevenDwellingsWithRooms() {return questionSevenDwellingsWithRooms;}
    public void setQuestionSevenDwellingsWithRooms(String questionSevenDwellingsWithRooms) {this.questionSevenDwellingsWithRooms = questionSevenDwellingsWithRooms;}
    public String getQuestionSevenRoomsPerHouse() {return questionSevenRoomsPerHouse;}
    public void setQuestionSevenRoomsPerHouse(String questionSevenRoomsPerHouse) {this.questionSevenRoomsPerHouse = questionSevenRoomsPerHouse;}

    public String getQuestionEight() {return questionEight;}
    public void setQuestionEight(String questionEight) {this.questionEight = questionEight;}

    public String getQuestionNine() {return questionNine;}
    public void setQuestionNine(String questionNine) {this.questionNine = questionNine;}
}
