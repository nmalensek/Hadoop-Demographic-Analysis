package cs455.hadoop.census.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

public class PlotResults {

    private Path outputPath;
    private FileSystem fileSystem;
    private Configuration configuration;
    private StateStrings stateStrings = new StateStrings();
    private XYSeries urbanPopulation = new XYSeries("Urban population");
    private XYSeries ruralPopulation = new XYSeries("Rural population");
    private XYSeries childrenUnder12 = new XYSeries("Non-hispanic children <= 11");
    private XYSeries children12To17 = new XYSeries("Non-hispanic children 12 - 17");
    private XYSeries hispanicChildrenUnder12 = new XYSeries("Hispanic children <= 11");
    private XYSeries hispanicChildren12To17 = new XYSeries("Hispanic children 12-17");
    private ApplicationFrame applicationFrame;

    public PlotResults(Path outputPath, Configuration configuration, String title) throws IOException {
        this.outputPath = outputPath;
        this.configuration = configuration;
        fileSystem = FileSystem.get(configuration);
        applicationFrame = new ApplicationFrame(title);
    }

    public void plotOutput() throws IOException {
        BufferedReader reader = null;
        try {
            FileStatus[] files = fileSystem.listStatus(outputPath);
            for (FileStatus status : files) {
                Path path = status.getPath();
                if (path.getName().contains("question9")) {
                    reader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
                    String line = reader.readLine();

                    while (line != null) {
                        addDataToChart(line);
                        line = reader.readLine();
                    }
                    createGraph();
                }
            }
        } finally {
            reader.close();
        }
    }

    private void addDataToChart(String line) {
        String[] splitLine = line.split(":");
        //ignore lines without data
        if (splitLine.length == 7) {
            for (int i = 0; i < splitLine.length; i++) {
                urbanPopulation.add(stateStrings.convertStateStringToInt(splitLine[0]), Double.parseDouble(splitLine[1]));
                ruralPopulation.add(stateStrings.convertStateStringToInt(splitLine[0]), Double.parseDouble(splitLine[2]));
                childrenUnder12.add(stateStrings.convertStateStringToInt(splitLine[0]), Double.parseDouble(splitLine[3]));
                children12To17.add(stateStrings.convertStateStringToInt(splitLine[0]), Double.parseDouble(splitLine[4]));
                hispanicChildrenUnder12.add(stateStrings.convertStateStringToInt(splitLine[0]), Double.parseDouble(splitLine[5]));
                hispanicChildren12To17.add(stateStrings.convertStateStringToInt(splitLine[0]), Double.parseDouble(splitLine[6]));
            }
        }
    }

    private void createGraph() {
        XYSeriesCollection data = new XYSeriesCollection();
        data.addSeries(urbanPopulation);
        data.addSeries(ruralPopulation);
        data.addSeries(childrenUnder12);
        data.addSeries(children12To17);
        data.addSeries(hispanicChildrenUnder12);
        data.addSeries(hispanicChildren12To17);

        JFreeChart chart = ChartFactory.createScatterPlot("XY Series", "State", "Percentage", data,
                PlotOrientation.VERTICAL, true, true, false);

        ChartPanel chartPanel = new ChartPanel(chart);
        chartPanel.setPreferredSize(new java.awt.Dimension(1000, 270));
        applicationFrame.setContentPane(chartPanel);
        applicationFrame.pack();
        RefineryUtilities.centerFrameOnScreen(applicationFrame);
        applicationFrame.setVisible(true);
    }
}
