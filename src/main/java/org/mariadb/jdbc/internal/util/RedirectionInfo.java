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
 */

package org.mariadb.jdbc.internal.util;

import org.mariadb.jdbc.*;

import java.sql.*;
import java.time.*;

public class RedirectionInfo {
  private final HostAddress host;
  private final String user;
  private final LocalDateTime timeout;

  public RedirectionInfo(HostAddress host, String user) {
    this.host = host;
    this.user = user;
    this.timeout = LocalDateTime.now().plusMinutes(10);
  }

  public HostAddress getHost() {
    return host;
  }

  public String getUser() {
    return user;
  }

  public boolean isValid() {
    return timeout.isAfter(LocalDateTime.now());
  }

  /**
   * Parse redirection info from a message return by server.
   *
   * @param msg the string which may contain redirection information.
   * @return RedirectionInfo host and user for redirection.
   */
  public static RedirectionInfo parseRedirectionInfo(String msg) throws SQLException {
    /**
     * Get redirected server information contained in OK packet. Redirection string somehow look
     * like: Location: mysql|mariadb://redirectedHostName:redirectedPort/user=redirectedUser
     *
     * @param str message about server information
     */
    String host;
    String user = null;
    int port = 3306;

    if (msg != null && msg.startsWith("Location: mysql://")
        || msg.startsWith("Location: " + "mariadb://")) {
      int prefixEnd = msg.startsWith("Location: mariadb://") ? 20 : 18;
      int afterHost = msg.indexOf("/", prefixEnd);
      if (afterHost != -1) {
        int portSeparation = msg.indexOf(":", prefixEnd);
        if (portSeparation != -1) {
          host = msg.substring(prefixEnd, portSeparation);
          try {
            port = Integer.parseInt(msg.substring(portSeparation + 1, afterHost));
          } catch (NumberFormatException nfe) {
            throw new SQLException("wrong redirection port value for redirection " + ": " + msg);
          }
        } else {
          host = msg.substring(prefixEnd, afterHost);
        }
        if (msg.indexOf("user=", afterHost) != -1) {
          user = msg.substring(afterHost + 6);
          if (user.isEmpty()) user = null;
        } else {
          user = null;
        }

      } else {
        int portSeparation = msg.indexOf(":", prefixEnd);
        if (portSeparation != -1) {
          host = msg.substring(prefixEnd, portSeparation);
          try {
            port = Integer.parseInt(msg.substring(portSeparation + 1));
          } catch (NumberFormatException nfe) {
            throw new SQLException("wrong redirection port value for redirection " + ": " + msg);
          }
        } else {
          host = msg.substring(prefixEnd);
        }
      }
      if (host.isEmpty()) {
        throw new SQLException("wrong redirection host value for redirection " + ": " + msg);
      }
      return new RedirectionInfo(new HostAddress(host, port), user);
    }
    return null;
  }

  @Override
  public String toString() {
    return "RedirectionInfo{" + "host=" + host + ", user='" + user + '\'' + '}';
  }
}
