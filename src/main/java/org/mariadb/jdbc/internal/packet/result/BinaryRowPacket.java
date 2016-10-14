/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:

Copyright (c) 2009-2011, Marcus Eriksson

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

package org.mariadb.jdbc.internal.packet.result;

import org.mariadb.jdbc.internal.packet.dao.ColumnInformation;
import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.queryresults.resultset.RowStore;
import org.mariadb.jdbc.internal.stream.MariaDbInputStream;
import org.mariadb.jdbc.internal.util.buffer.Buffer;

import java.io.IOException;
import java.io.InputStream;


public class BinaryRowPacket implements RowPacket {
    private final ColumnInformation[] columnInformation;
    private final int columnInformationLength;

    /**
     * Constructor.
     *
     * @param columnInformation       column information.
     * @param columnInformationLength number of columns
     */
    public BinaryRowPacket(ColumnInformation[] columnInformation, int columnInformationLength) {
        this.columnInformation = columnInformation;
        this.columnInformationLength = columnInformationLength;
    }

    /**
     * Read value corresponding to a column number from row.
     *
     * @param row current row bytes
     * @param column column number ( first is 0)
     * @param columnInfo column informations
     * @param initColumn last query column for faster access
     * @param initPosition last query row position for faster access
     * @return rowStore indicating data bytes, or null if data is null
     */
    public RowStore getOffsetAndLength(byte[] row, int column, ColumnInformation columnInfo, int initColumn, int initPosition) {

        int toReadLen;
        int position = 1;
        int nullCount = (columnInformationLength + 9) / 8;
        byte[] nullBitsBuffer = new byte[nullCount];
        for (int i = 0; i < nullCount; i++) {
            nullBitsBuffer[i] = row[position++];
        }

        if (initPosition != 0) position = initPosition;

        for (int i = initColumn; i < columnInformationLength; i++) {
            if ((nullBitsBuffer[(i + 2) / 8] & (1 << ((i + 2) % 8))) > 0) {
                //field is null
                if (i == column) return null;
            } else {
                switch (columnInformation[i].getType()) {
                    case VARCHAR:
                    case BIT:
                    case ENUM:
                    case SET:
                    case TINYBLOB:
                    case MEDIUMBLOB:
                    case LONGBLOB:
                    case BLOB:
                    case VARSTRING:
                    case STRING:
                    case GEOMETRY:
                    case OLDDECIMAL:
                    case DECIMAL:
                    case TIME:
                    case DATE:
                    case DATETIME:
                    case TIMESTAMP:
                        int read = row[position++] & 0xff;
                        switch (read) {
                            case 251:
                                if (i == column) return null;
                                break;
                            case 252:
                                toReadLen = ((row[position++] & 0xff) + ((row[position++] & 0xff) << 8));
                                if (i == column) {
                                    return new RowStore(row, position, toReadLen, columnInfo, column);
                                } else {
                                    position += toReadLen;
                                }
                                break;
                            case 253:
                                toReadLen = (row[position++] & 0xff)
                                        + ((row[position++] & 0xff) << 8)
                                        + ((row[position++] & 0xff) << 16);
                                if (i == column) {
                                    return new RowStore(row, position, toReadLen, columnInfo, column);
                                } else {
                                    position += toReadLen;
                                }
                                break;
                            case 254:
                                toReadLen = (int) (((row[position++] & 0xff)
                                        + ((long) (row[position++] & 0xff) << 8)
                                        + ((long) (row[position++] & 0xff) << 16)
                                        + ((long) (row[position++] & 0xff) << 24)
                                        + ((long) (row[position++] & 0xff) << 32)
                                        + ((long) (row[position++] & 0xff) << 40)
                                        + ((long) (row[position++] & 0xff) << 48)
                                        + ((long) (row[position++] & 0xff) << 56)));
                                if (i == column) {
                                    return new RowStore(row, position, toReadLen, columnInfo, column);
                                } else {
                                    position += toReadLen;
                                }
                                break;
                            default:
                                if (i == column) {
                                    return new RowStore(row, position, read, columnInfo, column);
                                } else {
                                    position += read;
                                }
                        }
                        break;

                    case BIGINT:
                    case DOUBLE:
                        if (i == column) return new RowStore(row, position, 8, columnInfo, column);
                        position += 8;
                        break;

                    case INTEGER:
                    case MEDIUMINT:
                    case FLOAT:
                        if (i == column) return new RowStore(row, position, 4, columnInfo, column);
                        position += 4;
                        break;

                    case SMALLINT:
                    case YEAR:
                        if (i == column) return new RowStore(row, position, 2, columnInfo, column);
                        position += 2;
                        break;

                    case TINYINT:
                        if (i == column) return new RowStore(row, position, 1, columnInfo, column);
                        position += 1;
                        break;
                    default:
                        if (i == column) return null;
                        break;
                }
            }
        }

        return null;
    }

}