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
            }

            if(logicalPartNumber == 2) {

                //question 1: rent vs. own
                int own = Integer.parseInt(line.substring(1803, 1812));
                int rent = Integer.parseInt(line.substring(1812, 1821));
                mapMultiple.setOwn(own);
                mapMultiple.setRent(rent);
            }

            context.write(new Text(state), mapMultiple);
        }
    }
}
