//package cs455.hadoop.census.util;
//
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.RecordWriter;
//import org.apache.hadoop.mapreduce.TaskAttemptContext;
//
//import java.io.DataOutputStream;
//import java.io.IOException;
//import java.util.List;
//
//public class OutputFormatter extends RecordWriter<Text, List<MapMultiple>> implements QuestionTitles {
//
//    private DataOutputStream outputStream;
//    private int questionCounter = 0;
//    private int stateCounter = 0;
//
//    public OutputFormatter(DataOutputStream outputStream) {
//        this.outputStream = outputStream;
//        try {
//            outputStream.writeBytes("\n\r");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Override
//    public void write(Text text, List<MapMultiple> mapMultiples) throws IOException, InterruptedException {
//        if (stateCounter == 10) {
//            stateCounter = 0;
//            questionCounter++;
//        } else {
//            //write key
//            outputStream.writeBytes(text.toString() + ": ");
//            writeAnswer(mapMultiples);
//            outputStream.writeBytes("\r\n");
//            stateCounter++;
//        }
//    }
//
//    @Override
//    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
//        outputStream.close();
//    }
//
//    private void writeAnswer(List<MapMultiple> reducerOutput) throws IOException {
//        switch(questionCounter) {
//            case POPULATION:
//                outputStream.writeBytes(Integer.toString(reducerOutput.get(0).getPopulation()));
//                break;
//            case QUESTION_ONE:
//                outputStream.writeBytes(Integer.toString(reducerOutput.get(0).getPopulation()));
//                break;
//        }
//    }
//}
