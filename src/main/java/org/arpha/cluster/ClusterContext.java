package org.arpha.cluster;

public class ClusterContext {

    private static ClusterManager MANAGER;

    public static void set(ClusterManager manager) {
        MANAGER = manager;
    }

    public static ClusterManager get() {
        if (MANAGER == null){
            throw new IllegalStateException("ClusterManager not initialized");
        }

        return MANAGER;
    }

}
