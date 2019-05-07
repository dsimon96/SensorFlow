package org.apache.storm.starter;

import org.apache.commons.cli.*;

public class SensorFlowMain {
    private static final Option EDGE_OPT = Option.builder("edge")
            .desc("Operate in edge mode")
            .build();
    private static final Option CLOUD_OPT = Option.builder("cloud")
            .desc("Operate in cloud mode")
            .build();
    private static final Option DEBUG_OPT = Option.builder("debug")
            .desc("Enable debug print statements")
            .build();
    private static final Option HOST_OPT = Option.builder("host")
            .hasArg(true)
            .type(String.class)
            .desc("Hostname for the other paired device")
            .build();
    private static final Option PORT_OPT = Option.builder("port")
            .hasArg(true)
            .type(Number.class)
            .desc("If in cloud mode, port to bind")
            .build();

    private static final Options CLI_OPTIONS = new Options()
            .addOption(EDGE_OPT)
            .addOption(CLOUD_OPT)
            .addOption(DEBUG_OPT)
            .addOption(PORT_OPT)
            .addOption(HOST_OPT);

    public static void main(String[] args) {
        CommandLineParser parser = new DefaultParser();

        boolean isServer;
        String host;
        int port;
        boolean debug;
        try {
            CommandLine cmd = parser.parse(CLI_OPTIONS, args);
            if (!validateArgs(cmd)) {
                HelpFormatter hf = new HelpFormatter();
                hf.printHelp("sensorflow", CLI_OPTIONS);
                return;
            }

            isServer = cmd.hasOption("cloud");
            port = ((Number) cmd.getParsedOptionValue("port")).intValue();
            host = (String)cmd.getParsedOptionValue("host");
            debug = cmd.hasOption("debug");
        } catch (ParseException e) {
            System.out.println("Failed to parse command line args!");
            return;
        }

        if (isServer) {
            final SensorFlowServer server = new SensorFlowServer(host, port, debug);
            try {
                server.start();
                server.blockUntilShutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            final SensorFlowClient client = new SensorFlowClient(host, port, debug);
            client.start();
        }
    }

    private static boolean validateArgs(CommandLine cmd) {
        if (!cmd.hasOption("cloud") && !cmd.hasOption("edge")) {
            System.out.println("Error: Please specify either cloud or edge mode.");
            return false;
        }

        if (!cmd.hasOption("host")) {
            System.out.println("Error: Please specify the hostname for the paired device.");
            return false;
        }

        if (!cmd.hasOption("port")) {
            System.out.println("Error: Please specify the port for the cloud server.");
            return false;
        }

        return true;
    }
}
