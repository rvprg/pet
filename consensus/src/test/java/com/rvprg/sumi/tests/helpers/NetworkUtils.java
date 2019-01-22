package com.rvprg.sumi.tests.helpers;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class NetworkUtils {
    private final static Random random = new Random();
    private final static int MAX_PORT = 65535;
    private final static int MIN_PORT = MAX_PORT / 2;

    public static boolean isPortFree(int port) {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            return true;
        } catch (IOException e) {
        } finally {
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException ex) {
                }
            }
        }
        return false;
    }

    public static Set<Integer> getRandomFreePorts(int howMany) {
        Set<Integer> freePorts = new HashSet<>();
        for (int i = 0; i < howMany; ++i) {
            while (true) {
                int port = getRandomFreePort(MIN_PORT, MAX_PORT);
                if (!freePorts.contains(port)) {
                    freePorts.add(port);
                    break;
                }
            }
        }
        return freePorts;
    }

    public static int getRandomFreePort() {
        return getRandomFreePort(MIN_PORT, MAX_PORT);
    }

    public static int getRandomFreePort(int minPort, int maxPort) {
        int port = 0;
        boolean found = false;
        do {
            port = getRandomPortNumber(minPort, maxPort);
            found = isPortFree(port);
        } while (!found);
        return port;
    }

    private static int getRandomPortNumber(int minPort, int maxPort) {
        if (maxPort < minPort) {
            throw new IllegalArgumentException();
        }
        return random.nextInt(maxPort - minPort) + minPort;
    }
}
