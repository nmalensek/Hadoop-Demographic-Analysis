package cs455.hadoop.census;

import cs455.hadoop.census.util.MapMultiple;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CensusDataCombiner extends Reducer<Text, MapMultiple, Text, MapMultiple> {

    @Override
    protected void reduce(Text key, Iterable<MapMultiple> values, Context context) throws IOException, InterruptedException {
        MapMultiple mapMultiple = new MapMultiple();

        double totalRent = 0;
        double totalOwn = 0;
        double totalMalesNeverMarried = 0;
        double totalFemalesNeverMarried = 0;
        double totalMarriageableMales = 0;
        double totalMarriageableFemales = 0;
        double insideUrban = 0;
        double outsideUrban = 0;
        double rural = 0;
        double hispanicMalesUnder18 = 0;
        double hispanicFemalesUnder18 = 0;
        double hispanicMales19to29 = 0;
        double hispanicFemales19to29 = 0;
        double hispanicMales30to39 = 0;
        double hispanicFemales30to39 = 0;
        double totalHispanicPopulation = 0;


        for (MapMultiple val : values) {
            totalRent += val.getRent();
            totalOwn += val.getOwn();
            totalMalesNeverMarried += val.getMaleNeverMarried();
            totalFemalesNeverMarried += val.getFemaleNeverMarried();
            totalMarriageableMales += val.getMarriageableMales();
            totalMarriageableFemales += val.getMarriageableFemales();
            insideUrban += val.getInsideUrban();
            outsideUrban += val.getOutsideUrban();
            rural += val.getRural();
            hispanicMalesUnder18 += val.getHispanicMalesUnder18();
            hispanicFemalesUnder18 += val.getHispanicFemalesUnder18();
            hispanicMales19to29 += val.getHispanicMales19to29();
            hispanicFemales19to29 += val.getHispanicFemales19to29();
            hispanicMales30to39 += val.getHispanicMales30to39();
            hispanicFemales30to39 += val.getHispanicFemales30to39();
            totalHispanicPopulation += val.getTotalHispanicPopulation();
        }

        mapMultiple.setRent(totalRent);
        mapMultiple.setOwn(totalOwn);
        mapMultiple.setMaleNeverMarried(totalMalesNeverMarried);
        mapMultiple.setFemaleNeverMarried(totalFemalesNeverMarried);
        mapMultiple.setMarriageableMales(totalMarriageableMales);
        mapMultiple.setMarriageableFemales(totalMarriageableFemales);
        mapMultiple.setInsideUrban(insideUrban);
        mapMultiple.setOutsideUrban(outsideUrban);
        mapMultiple.setRural(rural);
        mapMultiple.setHispanicMalesUnder18(hispanicMalesUnder18);
        mapMultiple.setHispanicFemalesUnder18(hispanicFemalesUnder18);
        mapMultiple.setHispanicMales19to29(hispanicMales19to29);
        mapMultiple.setHispanicFemales19to29(hispanicFemales19to29);
        mapMultiple.setHispanicMales30to39(hispanicMales30to39);
        mapMultiple.setHispanicFemales30to39(hispanicFemales30to39);
        mapMultiple.setTotalHispanicPopulation(totalHispanicPopulation);

        context.write(key, mapMultiple);
    }
}
