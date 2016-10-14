package org.mariadb.jdbc.internal.queryresults.resultset;

import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.packet.dao.ColumnInformation;

import java.nio.charset.StandardCharsets;

public class RowStore {

    private static final String zeroTimestamp = "0000-00-00 00:00:00";
    private static final String zeroDate = "0000-00-00";


    public final byte[] row;
    public ColumnInformation columnInfo;
    public int dataOffset;
    public int dataLength;

    //for next reads
    public int columnPosition;


    /**
     * Constructor of class which contain a data.
     * @param row row content.
     * @param dataOffset offset of data
     * @param dataLength data length
     * @param columnInfo data column information
     * @param columnPosition column position
     */
    public RowStore(byte[] row, int dataOffset, int dataLength, ColumnInformation columnInfo, int columnPosition) {
        this.row = row;
        this.dataOffset = dataOffset;
        this.dataLength = dataLength;
        this.columnInfo = columnInfo;
        this.columnPosition = columnPosition;
    }

    /**
     * Utility to check data is null according to column type.
     *
     * @param isBinaryEncoded is content stored as binary
     * @return true if null
     */
    public boolean isNull(boolean isBinaryEncoded) {
        if (isBinaryEncoded) {
            return (columnInfo.getType() == MariaDbType.DATE || columnInfo.getType() == MariaDbType.TIMESTAMP
                    || columnInfo.getType() == MariaDbType.DATETIME)
                    && dataLength == 0;
        } else {
            return ((columnInfo.getType() == MariaDbType.TIMESTAMP || columnInfo.getType() == MariaDbType.DATETIME)
                    && zeroTimestamp.equals(new String(row, dataOffset, dataLength, StandardCharsets.UTF_8)))
                    || (columnInfo.getType() == MariaDbType.DATE && zeroDate.equals(new String(row, dataOffset, dataLength, StandardCharsets.UTF_8)));
        }
    }
}
