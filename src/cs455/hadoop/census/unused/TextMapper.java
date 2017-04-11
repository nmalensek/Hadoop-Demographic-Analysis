package cs455.hadoop.census.unused;

import cs455.hadoop.census.ranges.HouseRanges;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class TextMapper extends Mapper<LongWritable, Text, Text, CustomWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final int analysisSummaryLevel = 100;
        CustomWritable customWritable = new CustomWritable();

        // tokenize into lines.
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        // emit word, count pairs.
        while (itr.hasMoreTokens()) {
            String line = itr.nextToken();
            String state = line.substring(8, 10);
            int lineSummaryLevel = Integer.parseInt(line.substring(10, 13));
            int logicalPartNumber = Integer.parseInt(line.substring(24, 28));

            if (lineSummaryLevel != analysisSummaryLevel) {
                continue; //abort mission, only looking at level 100
            }

            if (logicalPartNumber == 1) {
                //question 2: never married m, f. calculate from total state population acc. to instructor
                int totalPopulation = Integer.parseInt(line.substring(300, 309));
                int maleNeverMarried = Integer.parseInt(line.substring(4422, 4431));

                int femaleNeverMarried = Integer.parseInt(line.substring(4467, 4476));

                String neverMarriedPopulation =
                        totalPopulation + ":" + maleNeverMarried + ":" + femaleNeverMarried;
                customWritable.setQuestionTwo(neverMarriedPopulation);

                //question 3a males
                int hispanicMalesUnder18 = 0;
                int hispanicMaleStartPosition = 3864;
                for (int i = 0; i < 13; i++) {
                    hispanicMalesUnder18 += Integer.parseInt(
                            line.substring(hispanicMaleStartPosition, hispanicMaleStartPosition + 9));
                    hispanicMaleStartPosition += 9;
                }
                String malesUnder18 = String.valueOf(hispanicMalesUnder18);

                //question 3a females
                int hispanicFemalesUnder18 = 0;
                int hispanicFemaleStartPosition = 4143;
                for (int i = 0; i < 13; i++) {
                    hispanicFemalesUnder18 += Integer.parseInt(
                            line.substring(hispanicFemaleStartPosition, hispanicFemaleStartPosition + 9));
                    hispanicFemaleStartPosition += 9;
                }
                String femalesUnder18 = String.valueOf(hispanicFemalesUnder18);

                //question 3b males
                int hispanicMales19to29 = 0;
                int hispanicMale19to29StartPosition = 3981;
                for (int i = 0; i < 5; i++) {
                    hispanicMales19to29 += Integer.parseInt(
                            line.substring(hispanicMale19to29StartPosition, hispanicMale19to29StartPosition + 9));
                    hispanicMaleStartPosition += 9;
                }
                String males19to29 = String.valueOf(hispanicMales19to29);

                //question 3b females
                int hispanicFemales19to29 = 0;
                int hispanicFemale19to29StartPosition = 4260;
                for (int i = 0; i < 5; i++) {
                    hispanicFemales19to29 += Integer.parseInt(
                            line.substring(hispanicFemale19to29StartPosition, hispanicFemale19to29StartPosition + 9));
                    hispanicFemale19to29StartPosition += 9;
                }
                String females19to29 = String.valueOf(hispanicFemales19to29);

                //question 3c males
                int hispanicMales30to34 = Integer.parseInt(line.substring(4026, 4035));
                int hispanicMales35to39 = Integer.parseInt(line.substring(4035, 4044));
                int hispanicMales30to39 = hispanicMales30to34 + hispanicMales35to39;
                String males30to39 = String.valueOf(hispanicMales30to39);

                //question 3c females
                int hispanicFemales30to34 = Integer.parseInt(line.substring(4305, 4314));
                int hispanicFemales35to39 = Integer.parseInt(line.substring(4314, 4323));
                int hispanicFemales30to39 = hispanicFemales30to34 + hispanicFemales35to39;
                String females30to39 = String.valueOf(hispanicFemales30to39);

                //question 3 total hispanic population
                //62 is number of sections dedicated to hispanic population (31 each gender)
                int totalHispanicPopulation = 0;
                int hispanicPopulationStartPosition = 3864;
                for (int i = 0; i < 62; i++) {
                    totalHispanicPopulation += Integer.parseInt(
                            line.substring(hispanicPopulationStartPosition, hispanicPopulationStartPosition + 9));
                    hispanicPopulationStartPosition += 9;
                }

                customWritable.setQuestionThree(
                        totalHispanicPopulation + ":" +
                                malesUnder18 + ":" +
                                males19to29 + ":" +
                                males30to39 + ":" +
                                femalesUnder18 + ":" +
                                females19to29 + ":" +
                                females30to39
                );
            }

            if (logicalPartNumber == 2) {
                //question 1: rent vs. own
                int rent = Integer.parseInt(line.substring(1812, 1821));
                int own = Integer.parseInt(line.substring(1803, 1812));

                String rentVsOwn = rent + ":" + own;
                customWritable.setQuestionOne(rentVsOwn);

                //question 4: urban vs rural
                int inUrban = Integer.parseInt(line.substring(1821, 1830));
                int outUrban = Integer.parseInt(line.substring(1830, 1839));
                int rural = Integer.parseInt(line.substring(1839, 1848));
                int totalUrban = inUrban + outUrban;

                String ruralAndUrban = rural + ":" + totalUrban;
                customWritable.setQuestionFour(ruralAndUrban);

                //question 5: median owner-occupied home value
                int houseValueStartPosition = 2928;
                int totalHomes = 0;
                String homeValues = "";
                for (int i = 0; i < 20; i++) {
                    totalHomes += Integer.parseInt(line.substring(houseValueStartPosition, houseValueStartPosition + 9));
                    homeValues += Double.parseDouble(line.substring(houseValueStartPosition, houseValueStartPosition + 9)) + ":";
                    houseValueStartPosition += 9;
                }

                customWritable.setQuestionFiveTotalHomes(String.valueOf(totalHomes));
                customWritable.setQuestionFiveHomeValues(homeValues);

                //question 6: median rent value
                int rentAmountStartPosition = 3450;
                int totalRenters = 0;
                for (int i = 0; i < 17; i++) {
                    totalRenters += Integer.parseInt(line.substring(rentAmountStartPosition, rentAmountStartPosition + 9));
                    rentAmountStartPosition += 9;
                }
                mapMultiple.setTotalRenters(totalRenters);
                mapMultiple.setRentValue0(Integer.parseInt(line.substring(3450, 3459)));
                mapMultiple.setRentValue1(Integer.parseInt(line.substring(3459, 3468)));
                mapMultiple.setRentValue2(Integer.parseInt(line.substring(3468, 3477)));
                mapMultiple.setRentValue3(Integer.parseInt(line.substring(3477, 3486)));
                mapMultiple.setRentValue4(Integer.parseInt(line.substring(3486, 3495)));
                mapMultiple.setRentValue5(Integer.parseInt(line.substring(3495, 3504)));
                mapMultiple.setRentValue6(Integer.parseInt(line.substring(3504, 3513)));
                mapMultiple.setRentValue7(Integer.parseInt(line.substring(3513, 3522)));
                mapMultiple.setRentValue8(Integer.parseInt(line.substring(3522, 3531)));
                mapMultiple.setRentValue9(Integer.parseInt(line.substring(3531, 3540)));
                mapMultiple.setRentValue10(Integer.parseInt(line.substring(3540, 3549)));
                mapMultiple.setRentValue11(Integer.parseInt(line.substring(3549, 3558)));
                mapMultiple.setRentValue12(Integer.parseInt(line.substring(3558, 3567)));
                mapMultiple.setRentValue13(Integer.parseInt(line.substring(3567, 3576)));
                mapMultiple.setRentValue14(Integer.parseInt(line.substring(3576, 3585)));
                mapMultiple.setRentValue15(Integer.parseInt(line.substring(3585, 3594)));
                mapMultiple.setRentValue16(Integer.parseInt(line.substring(3594, 3603)));
            }

            context.write(new Text(state), customWritable);
        }
    }
}
