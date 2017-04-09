package cs455.hadoop.census.util;

public class StateStrings {

    public String[] getStates() {return states;}

    private String[] states = {
            "AK",
            "AL",
            "AR",
            "AZ",
            "CA",
            "CO",
            "CT",
            "DC",
            "DE",
            "FL",
            "GA",
            "HI",
            "IA",
            "ID",
            "IL",
            "IN",
            "KS",
            "KY",
            "LA",
            "MA",
            "MD",
            "ME",
            "MI",
            "MN",
            "MO",
            "MS",
            "MT",
            "NC",
            "ND",
            "NE",
            "NH",
            "NJ",
            "NM",
            "NV",
            "NY",
            "OH",
            "OK",
            "OR",
            "PA",
            "PR",
            "RI",
            "SC",
            "SD",
            "TN",
            "TX",
            "UT",
            "VA",
            "VI",
            "VT",
            "WA",
            "WI",
            "WV",
            "WY"
    };

    public int convertStateStringToInt(String state) {
        for (int i = 0; i < states.length; i++) {
            if (states[i].equals(state)) {
                return i; //convert string into int placeholder for chart
            }
        }
        return -1;
    }
}
