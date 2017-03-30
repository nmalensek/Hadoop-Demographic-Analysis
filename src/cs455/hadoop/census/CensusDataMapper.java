package cs455.hadoop.census;

import cs455.hadoop.census.util.MapMultiple;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Census data indices start at 1, so remember to subtract 1 in all index positioning!
 * substring(startIndexInclusive, endIndexExclusive)
 */

public class CensusDataMapper extends Mapper<LongWritable, Text, Text, MapMultiple> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final int analysisSummaryLevel = 100;
        MapMultiple mapMultiple = new MapMultiple();

        // tokenize into lines.
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        // emit word, count pairs.
        while (itr.hasMoreTokens()) {
            String line = itr.nextToken();
            String state = line.substring(8, 10);
            int lineSummaryLevel = Integer.parseInt(line.substring(10, 13));
            int logicalPartNumber = Integer.parseInt(line.substring(24, 28));

            if(lineSummaryLevel != analysisSummaryLevel) {
                continue; //abort mission, only looking at level 100
            }

            if(logicalPartNumber == 1) {
                //question 2: never married m, f
                int totalMarriageableMales = 0;
                int maleNeverMarried = Integer.parseInt(line.substring(4422, 4431));
                int maleMarried = Integer.parseInt(line.substring(4431, 4440));
                int maleSeparated = Integer.parseInt(line.substring(4440, 4449));
                int maleWidowed = Integer.parseInt(line.substring(4449, 4458));
                totalMarriageableMales += maleNeverMarried + maleMarried + maleSeparated + maleWidowed;

                int totalMarriageableFemales = 0;
                int femaleNeverMarried = Integer.parseInt(line.substring(4467, 4476));
                int femaleMarried = Integer.parseInt(line.substring(4476, 4485));
                int femaleSeparated = Integer.parseInt(line.substring(4485, 4494));
                int femaleWidowed = Integer.parseInt(line.substring(4494, 4503));
                totalMarriageableFemales += femaleNeverMarried + femaleMarried + femaleSeparated + femaleWidowed;

                mapMultiple.setMaleNeverMarried(maleNeverMarried);
                mapMultiple.setMarriageableMales(totalMarriageableMales);
                mapMultiple.setFemaleNeverMarried(femaleNeverMarried);
                mapMultiple.setMarriageableFemales(totalMarriageableFemales);

                //question 3a males
                int hispanicMalesUnder18 = 0;
                int hispanicMaleStartPosition = 3864;
                for (int i = 0; i < 13; i++) {
                    hispanicMalesUnder18 += Integer.parseInt(
                            line.substring(hispanicMaleStartPosition, hispanicMaleStartPosition + 9));
                    hispanicMaleStartPosition += 9;
                }
                mapMultiple.setHispanicMalesUnder18(hispanicMalesUnder18);

                //question 3a females
                int hispanicFemalesUnder18 = 0;
                int hispanicFemaleStartPosition = 4143;
                for (int i = 0; i < 13; i++) {
                    hispanicFemalesUnder18 += Integer.parseInt(
                            line.substring(hispanicFemaleStartPosition, hispanicFemaleStartPosition + 9));
                    hispanicFemaleStartPosition += 9;
                }
                mapMultiple.setHispanicFemalesUnder18(hispanicFemalesUnder18);
                

                //question 3b males
                int hispanicMales19to29 = 0;
                int hispanicMale19to29StartPosition = 3981;
                for (int i = 0; i < 5; i++) {
                    hispanicMales19to29 += Integer.parseInt(
                            line.substring(hispanicMale19to29StartPosition, hispanicMale19to29StartPosition + 9));
                    hispanicMaleStartPosition += 9;
                }
                mapMultiple.setHispanicMales19to29(hispanicMales19to29);

                //question 3b females
                int hispanicFemales19to29 = 0;
                int hispanicFemale19to29StartPosition = 4260;
                for (int i = 0; i < 5; i++) {
                    hispanicFemales19to29 += Integer.parseInt(
                            line.substring(hispanicFemale19to29StartPosition, hispanicFemale19to29StartPosition + 9));
                    hispanicFemale19to29StartPosition += 9;
                }
                mapMultiple.setHispanicFemales19to29(hispanicFemales19to29);

                //question 3c males
                int hispanicMales30to34 = Integer.parseInt(line.substring(4026, 4035));
                int hispanicMales35to39 = Integer.parseInt(line.substring(4035, 4044));
                int hispanicMales30to39 = hispanicMales30to34 + hispanicMales35to39;
                mapMultiple.setHispanicMales30to39(hispanicMales30to39);

                //question 3c females
                int hispanicFemales30to34 = Integer.parseInt(line.substring(4305, 4314));
                int hispanicFemales35to39 = Integer.parseInt(line.substring(4314, 4323));
                int hispanicFemales30to39 = hispanicFemales30to34 + hispanicFemales35to39;
                mapMultiple.setHispanicFemales30to39(hispanicFemales30to39);

                //question 3 total hispanic population
                //62 is number of sections dedicated to hispanic population (31 each gender)
                int totalHispanicPopulation = 0;
                int hispanicPopulationStartPosition = 3864;
                for (int i = 0; i < 62; i++) {
                    totalHispanicPopulation += Integer.parseInt(
                            line.substring(hispanicPopulationStartPosition, hispanicPopulationStartPosition + 9));
                    hispanicPopulationStartPosition += 9;
                }
                mapMultiple.setTotalHispanicPopulation(totalHispanicPopulation);
            }

            if(logicalPartNumber == 2) {
                //question 1: rent vs. own
                int own = Integer.parseInt(line.substring(1803, 1812));
                int rent = Integer.parseInt(line.substring(1812, 1821));
                mapMultiple.setOwn(own);
                mapMultiple.setRent(rent);

                //question 4: urban vs rural
                int inUrban = Integer.parseInt(line.substring(1821, 1830));
                int outUrban = Integer.parseInt(line.substring(1830, 1839));
                int rural = Integer.parseInt(line.substring(1839, 1848));
                mapMultiple.setInsideUrban(inUrban);
                mapMultiple.setOutsideUrban(outUrban);
                mapMultiple.setRural(rural);
            }

            context.write(new Text(state), mapMultiple);
        }
    }
}
