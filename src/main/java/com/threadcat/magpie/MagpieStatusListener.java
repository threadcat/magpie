package com.threadcat.magpie;

/**
 * @author threadcat
 */
public interface MagpieStatusListener {
    void statusChanged(String endpointId, MagpieStatus status);
}
