package hadoop.data.analysis.original;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Census data indices start at 1, so remember to subtract 1 in all index positioning!
 * substring(startIndexInclusive, endIndexExclusive)
 */

public class IndividualDataMapper extends Mapper<LongWritable, Text, Text, MapMultiple> {

    /**
     * Map program that splits text files and extracts relevant text values. Values are then set
     * in a custom Writable object (MapMultiple).
     * @param key state
     * @param value text value of specified substring
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */
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
                //question 2: never married m, f. comparing to total state population acc. to instructor direction.
                int population = Integer.parseInt(line.substring(300, 309));

                int maleNeverMarried = Integer.parseInt(line.substring(4422, 4431));
                int femaleNeverMarried = Integer.parseInt(line.substring(4467, 4476));

                mapMultiple.setPopulation(population);
                mapMultiple.setMaleNeverMarried(maleNeverMarried);
                mapMultiple.setFemaleNeverMarried(femaleNeverMarried);

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

                //question 8 % age > 85
                int elderlyPopulation = Integer.parseInt(line.substring(1065, 1074));
                mapMultiple.setElderlyPopulation(elderlyPopulation);

                //question 9 custom: how does percentage of urban vs. rural population affect the percentage of population
                //aged <1-11 and 12-17 with hispanic and non-hispanic backgrounds?

                int urbanPopulation = Integer.parseInt(line.substring(327, 336));
                int ruralPopulation = Integer.parseInt(line.substring(336, 345));
                mapMultiple.setUrbanPopulation(urbanPopulation);
                mapMultiple.setRuralPopulation(ruralPopulation);

                int childrenUnder1To11 = 0;
                int smallChildrenStartPosition = 795;
                for (int i = 0; i < 7; i++) {
                    childrenUnder1To11 += Integer.parseInt(
                            line.substring(smallChildrenStartPosition, smallChildrenStartPosition + 9));
                    smallChildrenStartPosition += 9;
                }
                mapMultiple.setChildrenUnder1To11(childrenUnder1To11);

                int children12To17 = 0;
                int teenChildrenStartPosition = 858;
                for (int i = 0; i < 5; i++) {
                    children12To17 += Integer.parseInt(
                            line.substring(teenChildrenStartPosition, teenChildrenStartPosition + 9));
                    teenChildrenStartPosition += 9;
                }
                mapMultiple.setChildren12To17(children12To17);

                int hispanicMalesUnder1To11 = 0;
                int hispanicChildrenStartPosition = 3864;
                for (int i = 0; i < 7; i++) {
                    hispanicMalesUnder1To11 += Integer.parseInt(
                            line.substring(hispanicChildrenStartPosition, hispanicChildrenStartPosition + 9));
                    hispanicChildrenStartPosition += 9;
                }

                int hispanicFemalesUnder1To11 = 0;
                int hispanicFemaleChildrenStartPosition = 3864;
                for (int i = 0; i < 7; i++) {
                    hispanicFemalesUnder1To11 += Integer.parseInt(
                            line.substring(hispanicFemaleChildrenStartPosition, hispanicFemaleChildrenStartPosition + 9));
                    hispanicFemaleChildrenStartPosition += 9;
                }

                mapMultiple.setHispanicChildrenUnder1To11(hispanicMalesUnder1To11 + hispanicFemalesUnder1To11);

                int hispanicMales12To17 = 0;
                int hispanicMales12To17Start = 3927;
                for (int i = 0; i < 5; i++) {
                    hispanicMales12To17 += Integer.parseInt(
                            line.substring(hispanicMales12To17Start, hispanicMales12To17Start + 9));
                    hispanicMales12To17Start += 9;
                }

                int hispanicFemales12To17 = 0;
                int hispanicFemales12To17Start = 3927;
                for (int i = 0; i < 5; i++) {
                    hispanicFemales12To17 += Integer.parseInt(
                            line.substring(hispanicFemales12To17Start, hispanicFemales12To17Start + 9));
                    hispanicFemales12To17Start += 9;
                }

                mapMultiple.setHispanicChildren12To17(hispanicMales12To17 + hispanicFemales12To17);

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
                int notDefined = Integer.parseInt(line.substring(1848, 1857));
                mapMultiple.setInsideUrban(inUrban);
                mapMultiple.setOutsideUrban(outUrban);
                mapMultiple.setRural(rural);
                mapMultiple.setNotDefined(notDefined);

                //question 5: median owner-occupied home value
                int houseValueStartPosition = 2928;
                int totalHomes = 0;
                for (int i = 0; i < 20; i++) {
                    totalHomes += Integer.parseInt(line.substring(houseValueStartPosition, houseValueStartPosition + 9));
                    houseValueStartPosition += 9;
                }
                mapMultiple.setTotalOwnedHomes(totalHomes);
                mapMultiple.setOwnedHomeValue0(Integer.parseInt(line.substring(2928, 2937)));
                mapMultiple.setOwnedHomeValue1(Integer.parseInt(line.substring(2937, 2946)));
                mapMultiple.setOwnedHomeValue2(Integer.parseInt(line.substring(2946, 2955)));
                mapMultiple.setOwnedHomeValue3(Integer.parseInt(line.substring(2955, 2964)));
                mapMultiple.setOwnedHomeValue4(Integer.parseInt(line.substring(2964, 2973)));
                mapMultiple.setOwnedHomeValue5(Integer.parseInt(line.substring(2973, 2982)));
                mapMultiple.setOwnedHomeValue6(Integer.parseInt(line.substring(2982, 2991)));
                mapMultiple.setOwnedHomeValue7(Integer.parseInt(line.substring(2991, 3000)));
                mapMultiple.setOwnedHomeValue8(Integer.parseInt(line.substring(3000, 3009)));
                mapMultiple.setOwnedHomeValue9(Integer.parseInt(line.substring(3009, 3018)));
                mapMultiple.setOwnedHomeValue10(Integer.parseInt(line.substring(3018, 3027)));
                mapMultiple.setOwnedHomeValue11(Integer.parseInt(line.substring(3027, 3036)));
                mapMultiple.setOwnedHomeValue12(Integer.parseInt(line.substring(3036, 3045)));
                mapMultiple.setOwnedHomeValue13(Integer.parseInt(line.substring(3045, 3054)));
                mapMultiple.setOwnedHomeValue14(Integer.parseInt(line.substring(3054, 3063)));
                mapMultiple.setOwnedHomeValue15(Integer.parseInt(line.substring(3063, 3072)));
                mapMultiple.setOwnedHomeValue16(Integer.parseInt(line.substring(3072, 3081)));
                mapMultiple.setOwnedHomeValue17(Integer.parseInt(line.substring(3081, 3090)));
                mapMultiple.setOwnedHomeValue18(Integer.parseInt(line.substring(3090, 3099)));
                mapMultiple.setOwnedHomeValue19(Integer.parseInt(line.substring(3099, 3108)));

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

                //question 7: 95th percentile of average rooms per house
                int roomStartPosition = 2388;
                int totalRooms = 0;
                for (int i = 0; i < 9; i++) {
                    totalRooms += Integer.parseInt(line.substring(roomStartPosition, roomStartPosition + 9));
                    roomStartPosition += 9;
                }
                mapMultiple.setTotalRooms(totalRooms);
                mapMultiple.setOneRoom(Integer.parseInt(line.substring(2388, 2397)));
                mapMultiple.setTwoRooms(Integer.parseInt(line.substring(2397, 2406)));
                mapMultiple.setThreeRooms(Integer.parseInt(line.substring(2406, 2415)));
                mapMultiple.setFourRooms(Integer.parseInt(line.substring(2415, 2424)));
                mapMultiple.setFiveRooms(Integer.parseInt(line.substring(2424, 2433)));
                mapMultiple.setSixRooms(Integer.parseInt(line.substring(2433, 2442)));
                mapMultiple.setSevenRooms(Integer.parseInt(line.substring(2442, 2451)));
                mapMultiple.setEightRooms(Integer.parseInt(line.substring(2451, 2460)));
                mapMultiple.setNineRooms(Integer.parseInt(line.substring(2460, 2469)));

            }

            context.write(new Text(state), mapMultiple);
        }
    }
}
