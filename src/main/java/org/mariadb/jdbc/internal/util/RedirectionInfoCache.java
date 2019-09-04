/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2019 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 *
 */

package org.mariadb.jdbc.internal.util;

import org.mariadb.jdbc.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public final class RedirectionInfoCache extends ConcurrentHashMap<String, RedirectionInfo> {
  private final AtomicInteger maxSize;

  public RedirectionInfoCache(int sz) {
        maxSize = new AtomicInteger(sz);
    }

    public static RedirectionInfoCache newInstance(int maxSize) {
        return new RedirectionInfoCache(maxSize);
    }

    private String makeKey(String user, HostAddress host) {
        return user + "_" + host.host + "_" + host.port;
    }

    /**
    * Get redirection info from cache.
    *
    * @param user  original connection user.
    * @param host  original connection host.
    * @return RedirectionInfo host and user for redirection.
    */
    public RedirectionInfo get(String user, HostAddress host) {
        String key = makeKey(user, host);
        RedirectionInfo redirectionInfo = super.get(key);
        if (redirectionInfo != null && !redirectionInfo.isValid()) {
            super.remove(key);
            return null;
        }
        return redirectionInfo;
    }

    /**
    * Put redirection info into cache.
    *
    * @param user           original connection user.
    * @param host           original connection host.
    * @param redirectionInfo   redirection connection information.
    */
    public void put(String user, HostAddress host, RedirectionInfo redirectionInfo) {
        String key = makeKey(user, host);
        if (maxSize.get() == -1 ||  super.size() < maxSize.get()) {
            super.put(key, redirectionInfo);
        } else {
          //eat exception, use redirection info without caching it
        }
    }

    /**
    * remove redirection info into cache.
    *
     * @param redirectionInfo   redirection connection information.
    */
    public void remove(RedirectionInfo redirectionInfo) {
        String key = makeKey(redirectionInfo.getUser(), redirectionInfo.getHost());
        super.remove(key);
    }
}
