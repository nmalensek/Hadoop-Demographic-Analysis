package cs455.hadoop.census.ranges;

public class HouseRanges implements Ranges {

    private static final HouseRanges instance = new HouseRanges();

    private HouseRanges() {}

    public static HouseRanges getInstance() {return instance;}

    String[] housing = {"under $15,000",
                        "$15,000 - $19,999",
                        "$20,000 - $24,999",
                        "$25,000 - $29,999",
                        "$30,000 - $34,999",
                        "$35,000 - $39,999",
                        "$40,000 - $44,999",
                        "$45,000 - $49,999",
                        "$50,000 - $59,999",
                        "$60,000 - $74,999",
                        "$75,000 - $99,999",
                        "$100,000 - $124,999",
                        "$125,000 - $149,999",
                        "$150,000 - $174,999",
                        "$175,000 - $199,999",
                        "$200,000 - $249,999",
                        "$250,000 - $299,999",
                        "$300,000 - $399,999",
                        "$400,000 - $499,999",
                        "$500,000 or more"};

    public String[] getRanges() {return housing;}

}
