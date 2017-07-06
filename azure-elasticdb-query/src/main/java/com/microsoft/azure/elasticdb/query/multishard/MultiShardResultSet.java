package com.microsoft.azure.elasticdb.query.multishard;

/* Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.query.exception.MultiShardException;
import com.microsoft.azure.elasticdb.query.exception.MultiShardResultSetClosedException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

//TODO: Test all the methods of this class
public class MultiShardResultSet implements AutoCloseable, ResultSet {

  private List<LabeledResultSet> results;
  private int currentIndex;
  private LabeledResultSet currentResultSet;

  /**
   * Creates an Instance of MultiShardResultSet.
   *
   * @param results List of LabeledResultSet.
   */
  public MultiShardResultSet(List<LabeledResultSet> results) {
    this.results = results;
    this.currentIndex = 0;
  }

  @Override
  public void close() throws SQLException {
    for (LabeledResultSet result : results) {
      result.close();
    }
  }

  @Override
  public boolean next() throws SQLException {
    if (currentIndex > results.size()) {
      return false;
    }
    if (currentResultSet == null) {
      // This is the first time next is called.
      do {
        // Populate currentResultSet.
        currentResultSet = results.get(currentIndex);
        // Increment currentIndex.
        currentIndex++;
        // Do this until you get a result set which isn't null.
      } while (currentIndex < results.size() && currentResultSet.getResultSet() == null);

      // If we don't have result sets throw MultiShardResultSetClosedException exception
      if (currentResultSet.getResultSet() == null) {
        throw new MultiShardResultSetClosedException("Statement did not return ResultSet");
      }

      return currentResultSet.getResultSet().next();
    } else {
      ResultSet currentSet = currentResultSet.getResultSet();
      if (currentSet.next()) {
        return true;
      } else if (currentIndex < results.size()) {
        currentResultSet = results.get(currentIndex);
        currentIndex++;
        return currentResultSet.getResultSet() != null && currentResultSet.getResultSet().next();
      }
    }
    // We have reached the end of the result.
    return false;
  }

  @Override
  public boolean wasNull() throws SQLException {
    return false;
  }

  public List<LabeledResultSet> getResults() {
    return this.results;
  }

  private ResultSet getCurrentResultSet() {
    return currentResultSet.getResultSet();

  }

  public String getLocation() {
    return results.get(currentIndex - 1).getShardLabel();
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getString(columnIndex);
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getString(columnLabel);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getBoolean(columnIndex);
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getBoolean(columnLabel);
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getByte(columnIndex);
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getByte(columnLabel);
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getShort(columnIndex);
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getShort(columnLabel);
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getInt(columnIndex);
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getInt(columnLabel);
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getLong(columnIndex);
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getLong(columnLabel);
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getFloat(columnIndex);
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getFloat(columnLabel);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getDouble(columnIndex);
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getDouble(columnLabel);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getBigDecimal(columnIndex);
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getBigDecimal(columnLabel);
  }

  @Override
  @Deprecated
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    return this.getCurrentResultSet().getBigDecimal(columnIndex, scale);
  }

  @Override
  @Deprecated
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    return this.getCurrentResultSet().getBigDecimal(columnLabel, scale);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getBytes(columnIndex);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getBytes(columnLabel);
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getDate(columnIndex);
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getDate(columnLabel);
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    return this.getCurrentResultSet().getDate(columnIndex, cal);
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return this.getCurrentResultSet().getDate(columnLabel, cal);
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getTime(columnIndex);
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getTime(columnLabel);
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    return this.getCurrentResultSet().getTime(columnIndex, cal);
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return this.getCurrentResultSet().getTime(columnLabel, cal);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getTimestamp(columnIndex);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getTimestamp(columnLabel);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    return this.getCurrentResultSet().getTimestamp(columnIndex, cal);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return this.getCurrentResultSet().getTimestamp(columnLabel, cal);
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getAsciiStream(columnIndex);
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getAsciiStream(columnLabel);
  }

  @Override
  @Deprecated
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getUnicodeStream(columnIndex);
  }

  @Override
  @Deprecated
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getUnicodeStream(columnLabel);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getBinaryStream(columnIndex);
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getBinaryStream(columnLabel);
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return this.getCurrentResultSet().getWarnings();
  }

  @Override
  public String getCursorName() throws SQLException {
    return this.getCurrentResultSet().getCursorName();
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    if (this.results != null && this.results.size() > 0) {
      int i = 0;
      do {
        ResultSet r = this.results.get(i).getResultSet();
        if (r != null) {
          return r.getMetaData();
        }
        i++;
      } while (i < this.results.size());
    }
    return null;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getObject(columnIndex);
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getObject(columnLabel);
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    return this.getCurrentResultSet().getObject(columnIndex, map);
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    return this.getCurrentResultSet().getObject(columnLabel, map);
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    return this.getCurrentResultSet().getObject(columnIndex, type);
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return this.getCurrentResultSet().getObject(columnLabel, type);
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getCharacterStream(columnIndex);
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getCharacterStream(columnLabel);
  }

  @Override
  public int getRow() throws SQLException {
    int currentRow = getCurrentResultSet() == null ? 0 : getCurrentResultSet().getRow();
    int totalRowsOfPreviousResultSets = 0;
    if (currentIndex - 2 >= 0) {
      int index = currentIndex - 2;
      for (; index >= 0; index--) {
        ResultSet set = results.get(index).getResultSet();
        set.last();
        totalRowsOfPreviousResultSets += set.getRow();
      }
    }
    return currentRow + totalRowsOfPreviousResultSets;
  }

  /**
   * Get the total number of rows present in the current result set without disturbing the cursor.
   *
   * @return Total Number of Rows
   * @throws SQLException Exception thrown by any corrupted result set
   */
  public int getRowCount() throws SQLException {
    int totalCount = 0;
    if (results.size() > 0) {
      for (LabeledResultSet result : results) {
        ResultSet set = result.getResultSet();
        set.last();
        totalCount += set.getRow();
        set.beforeFirst();
      }
    }
    return totalCount;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return this.getCurrentResultSet().getFetchDirection();
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    for (LabeledResultSet result : results) {
      result.getResultSet().setFetchDirection(direction);
    }
  }

  @Override
  public int getFetchSize() throws SQLException {
    return getCurrentResultSet().getFetchSize();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    getCurrentResultSet().setFetchSize(rows);
  }

  @Override
  public int getType() throws SQLException {
    return this.getCurrentResultSet().getType();
  }

  @Override
  public int getConcurrency() throws SQLException {
    return this.getCurrentResultSet().getConcurrency();
  }

  @Override
  public Statement getStatement() throws SQLException {
    return this.getCurrentResultSet().getStatement();
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getRef(columnIndex);
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getRef(columnLabel);
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getBlob(columnIndex);
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getBlob(columnLabel);
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getClob(columnIndex);
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getClob(columnLabel);
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getArray(columnIndex);
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getArray(columnLabel);
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getURL(columnIndex);
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getURL(columnLabel);
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getRowId(columnIndex);
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getRowId(columnLabel);
  }

  @Override
  public int getHoldability() throws SQLException {
    return this.getCurrentResultSet().getHoldability();
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getNClob(columnIndex);
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getNClob(columnLabel);
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getNString(columnIndex);
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getNString(columnLabel);
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getNCharacterStream(columnIndex);
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getNCharacterStream(columnLabel);
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    return this.getCurrentResultSet().getSQLXML(columnIndex);
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    return this.getCurrentResultSet().getSQLXML(columnLabel);
  }

  @Override
  public void clearWarnings() throws SQLException {
    for (LabeledResultSet result : results) {
      result.getResultSet().clearWarnings();
    }
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    return getCurrentResultSet().findColumn(columnLabel);
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return getRow() == 0;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return getRow() == 0;
  }

  @Override
  public boolean isFirst() throws SQLException {
    return getRow() == 1;
  }

  @Override
  public boolean isLast() throws SQLException {
    return getRow() == getRowCount();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return false;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  @Override
  public void beforeFirst() throws SQLException {
    currentIndex = 0;
  }

  @Override
  public void afterLast() throws SQLException {
    currentIndex = results.size();
  }

  @Override
  public boolean first() throws SQLException {
    if (results.size() > 0) {
      currentIndex = 0;
      currentResultSet = results.get(currentIndex);
      return currentResultSet.getResultSet().first();
    }
    return false;
  }

  @Override
  public boolean last() throws SQLException {
    int resultSize = results.size();
    if (resultSize > 0) {
      currentIndex = resultSize - 1;
      currentResultSet = results.get(currentIndex);
      return currentResultSet.getResultSet().last();
    }
    return false;
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    int index = 0;
    for (LabeledResultSet currentSet : results) {
      if (row == currentSet.getResultSet().getRow()) {
        currentIndex = index;
        currentResultSet = currentSet;
        return getCurrentResultSet().next();
      }
      index++;
    }
    return false;
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    if (rows > 0) {
      while (rows-- > 0) {
        if (!(getCurrentResultSet().next())) {
          break;
        }
        getCurrentResultSet().next();
      }
    } else {
      while (rows < 0) {
        if (!(getCurrentResultSet().previous())) {
          break;
        }
        getCurrentResultSet().previous();
        rows += 1;
      }
    }
    return true;
  }

  @Override
  public boolean previous() throws SQLException {
    if (getCurrentResultSet().previous()) {
      return true;
    } else {
      if (currentIndex - 2 < 0) {
        currentIndex = currentIndex - 2;
        currentResultSet = results.get(currentIndex);
        return getCurrentResultSet().last();
      }
    }
    return false;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    return false;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    return false;
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    this.getCurrentResultSet().updateNull(columnIndex);
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    this.getCurrentResultSet().updateNull(columnLabel);
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    this.getCurrentResultSet().updateBoolean(columnIndex, x);
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    this.getCurrentResultSet().updateBoolean(columnLabel, x);
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    this.getCurrentResultSet().updateByte(columnIndex, x);
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    this.getCurrentResultSet().updateByte(columnLabel, x);
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    this.getCurrentResultSet().updateShort(columnIndex, x);
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    this.getCurrentResultSet().updateShort(columnLabel, x);
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    this.getCurrentResultSet().updateInt(columnIndex, x);
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    this.getCurrentResultSet().updateInt(columnLabel, x);
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    this.getCurrentResultSet().updateLong(columnIndex, x);
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    this.getCurrentResultSet().updateLong(columnLabel, x);
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    this.getCurrentResultSet().updateFloat(columnIndex, x);
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    this.getCurrentResultSet().updateFloat(columnLabel, x);
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    this.getCurrentResultSet().updateDouble(columnIndex, x);
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    this.getCurrentResultSet().updateDouble(columnLabel, x);
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    this.getCurrentResultSet().updateBigDecimal(columnIndex, x);
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    this.getCurrentResultSet().updateBigDecimal(columnLabel, x);
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    this.getCurrentResultSet().updateString(columnIndex, x);
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    this.getCurrentResultSet().updateString(columnLabel, x);
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    this.getCurrentResultSet().updateBytes(columnIndex, x);
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    this.getCurrentResultSet().updateBytes(columnLabel, x);
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    this.getCurrentResultSet().updateDate(columnIndex, x);
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    this.getCurrentResultSet().updateDate(columnLabel, x);
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    this.getCurrentResultSet().updateTime(columnIndex, x);
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    this.getCurrentResultSet().updateTime(columnLabel, x);
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    this.getCurrentResultSet().updateTimestamp(columnIndex, x);
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    this.getCurrentResultSet().updateTimestamp(columnLabel, x);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    this.getCurrentResultSet().updateAsciiStream(columnIndex, x);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    this.getCurrentResultSet().updateAsciiStream(columnLabel, x);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    this.getCurrentResultSet().updateAsciiStream(columnIndex, x);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    this.getCurrentResultSet().updateAsciiStream(columnLabel, x, length);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    this.getCurrentResultSet().updateAsciiStream(columnIndex, x);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    this.getCurrentResultSet().updateAsciiStream(columnLabel, x);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    this.getCurrentResultSet().updateBinaryStream(columnIndex, x, length);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    this.getCurrentResultSet().updateBinaryStream(columnLabel, x, length);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    this.getCurrentResultSet().updateBinaryStream(columnIndex, x, length);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    this.getCurrentResultSet().updateBinaryStream(columnLabel, x, length);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    this.getCurrentResultSet().updateBinaryStream(columnIndex, x);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    this.getCurrentResultSet().updateBinaryStream(columnLabel, x);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    this.getCurrentResultSet().updateCharacterStream(columnIndex, x, length);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    this.getCurrentResultSet().updateCharacterStream(columnIndex, x);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader x) throws SQLException {
    this.getCurrentResultSet().updateCharacterStream(columnLabel, x);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader x, int length)
      throws SQLException {
    this.getCurrentResultSet().updateCharacterStream(columnLabel, x, length);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    this.getCurrentResultSet().updateCharacterStream(columnIndex, x, length);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader x, long length)
      throws SQLException {
    this.getCurrentResultSet().updateCharacterStream(columnLabel, x, length);
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    this.getCurrentResultSet().updateBlob(columnIndex, x);
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    this.getCurrentResultSet().updateBlob(columnLabel, x);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    this.getCurrentResultSet().updateBlob(columnIndex, inputStream);
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    this.getCurrentResultSet().updateBlob(columnLabel, inputStream);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    this.getCurrentResultSet().updateBlob(columnIndex, inputStream, length);
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    this.getCurrentResultSet().updateBlob(columnLabel, inputStream, length);
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    this.getCurrentResultSet().updateClob(columnIndex, x);
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    this.getCurrentResultSet().updateClob(columnLabel, x);
  }

  @Override
  public void updateClob(int columnIndex, Reader x) throws SQLException {
    this.getCurrentResultSet().updateClob(columnIndex, x);
  }

  @Override
  public void updateClob(String columnLabel, Reader x) throws SQLException {
    this.getCurrentResultSet().updateClob(columnLabel, x);
  }

  @Override
  public void updateClob(int columnIndex, Reader x, long length) throws SQLException {
    this.getCurrentResultSet().updateClob(columnIndex, x, length);
  }

  @Override
  public void updateClob(String columnLabel, Reader x, long length) throws SQLException {
    this.getCurrentResultSet().updateClob(columnLabel, x, length);
  }

  @Override
  public void updateNClob(int columnIndex, Reader x, long length) throws SQLException {
    this.getCurrentResultSet().updateNClob(columnIndex, x, length);
  }

  @Override
  public void updateNClob(int columnIndex, NClob clob) throws SQLException {
    this.getCurrentResultSet().updateNClob(columnIndex, clob);
  }

  @Override
  public void updateNClob(String columnLabel, NClob clob) throws SQLException {
    this.getCurrentResultSet().updateNClob(columnLabel, clob);
  }

  @Override
  public void updateNClob(int columnIndex, Reader x) throws SQLException {
    this.getCurrentResultSet().updateNClob(columnIndex, x);
  }

  @Override
  public void updateNClob(String columnLabel, Reader x) throws SQLException {
    this.getCurrentResultSet().updateNClob(columnLabel, x);
  }

  @Override
  public void updateNClob(String columnLabel, Reader x, long length) throws SQLException {
    this.getCurrentResultSet().updateNClob(columnLabel, x, length);
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    this.getCurrentResultSet().updateNCharacterStream(columnIndex, x);
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader x) throws SQLException {
    this.getCurrentResultSet().updateNCharacterStream(columnLabel, x);
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    this.getCurrentResultSet().updateNCharacterStream(columnIndex, x, length);
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader x, long length)
      throws SQLException {
    this.getCurrentResultSet().updateNCharacterStream(columnLabel, x, length);
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    this.getCurrentResultSet().updateObject(columnIndex, x);
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    this.getCurrentResultSet().updateObject(columnLabel, x);
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
    this.getCurrentResultSet().updateObject(columnIndex, x, targetSqlType);
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType)
      throws SQLException {
    this.getCurrentResultSet().updateObject(columnLabel, x, targetSqlType);
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    this.getCurrentResultSet().updateObject(columnIndex, x, scaleOrLength);
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    this.getCurrentResultSet().updateObject(columnLabel, x, scaleOrLength);
  }

  @Override
  public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    this.getCurrentResultSet().updateObject(columnIndex, x, targetSqlType);
  }

  @Override
  public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength)
      throws SQLException {
    this.getCurrentResultSet().updateObject(columnLabel, x, targetSqlType, scaleOrLength);
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    this.getCurrentResultSet().updateRef(columnIndex, x);
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    this.getCurrentResultSet().updateRef(columnLabel, x);
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    this.getCurrentResultSet().updateArray(columnIndex, x);
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    this.getCurrentResultSet().updateArray(columnLabel, x);
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    this.getCurrentResultSet().updateRowId(columnIndex, x);
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    this.getCurrentResultSet().updateRowId(columnLabel, x);
  }

  @Override
  public void updateNString(int columnIndex, String string) throws SQLException {
    this.getCurrentResultSet().updateNString(columnIndex, string);
  }

  @Override
  public void updateNString(String columnLabel, String string) throws SQLException {
    this.getCurrentResultSet().updateNString(columnLabel, string);
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    this.getCurrentResultSet().updateSQLXML(columnIndex, xmlObject);
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    this.getCurrentResultSet().updateSQLXML(columnLabel, xmlObject);
  }

  @Override
  public void updateRow() throws SQLException {

  }

  @Override
  public void insertRow() throws SQLException {

  }

  @Override
  public void deleteRow() throws SQLException {

  }

  @Override
  public void refreshRow() throws SQLException {

  }

  @Override
  public void cancelRowUpdates() throws SQLException {

  }

  @Override
  public void moveToInsertRow() throws SQLException {

  }

  @Override
  public void moveToCurrentRow() throws SQLException {

  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  public List<MultiShardException> getMultiShardExceptions() {
    List<MultiShardException> exceptions = new ArrayList<>();
    if (this.results != null) {
      this.results.forEach(set -> {
        MultiShardException ex = set.getException();
        if (ex != null) {
          exceptions.add(ex);
        }
      });
    }
    return exceptions;
  }
}
