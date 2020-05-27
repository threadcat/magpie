package com.threadcat.magpie.channel;

import com.threadcat.magpie.MagpieStatus;

import java.net.InetSocketAddress;

/**
 * @author threadcat
 */
public interface MagpieChannelStatusListener {

    void statusChanged(InetSocketAddress address, MagpieStatus status);
}
