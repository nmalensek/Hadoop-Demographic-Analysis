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

        context.write(key, mapMultiple);
    }
}
