/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.closeStmtAndConn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UpsertCompareIT extends BaseClientManagedTimeIT {

    @Before
    public void setup() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("create table UpsertCompare (k varchar, value1 varchar, value2 varchar, value3 integer, constraint pk primary key(k))");
            stmt.execute();
        } finally {
            closeStmtAndConn(stmt, conn);
        }

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into UpsertCompare (k,value1,value2,value3) values (?, ?, ?, ?)");
            stmt.setString(1, "Key-1");
            stmt.setString(2, null);
            stmt.setString(3, "Compare Test");
            stmt.setInt(4, 1);
            stmt.executeUpdate();
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }

    @Test
    public void testCompareNullExpression() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into UpsertCompare (k,value3) values (?, ?) compare value1 is null");
            stmt.setString(1, "Key-1");
            stmt.setInt(2, 5);
            stmt.executeUpdate();
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select * from UpsertCompare");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Key-1", rs.getString(1));
            assertEquals(null, rs.getString(2));
            assertEquals("Compare Test", rs.getString(3));
            assertEquals(5, rs.getInt(4));
            assertFalse(rs.next());
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }

    @Test
    public void testCompareWithNullCellValue() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into UpsertCompare (k,value3) values (?, ?) compare value1='test'");
            stmt.setString(1, "Key-1");
            stmt.setInt(2, 5);
            stmt.executeUpdate();
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select * from UpsertCompare");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Key-1", rs.getString(1));
            assertEquals(null, rs.getString(2));
            assertEquals("Compare Test", rs.getString(3));
            assertEquals(1, rs.getInt(4));
            assertFalse(rs.next());
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }

    @Test
    public void testCompareFalseExpression() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into UpsertCompare (k,value3) values (?, ?) compare value2='root'");
            stmt.setString(1, "Key-1");
            stmt.setInt(2, 5);
            stmt.executeUpdate();
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select * from UpsertCompare");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Key-1", rs.getString(1));
            assertEquals(null, rs.getString(2));
            assertEquals("Compare Test", rs.getString(3));
            assertEquals(1, rs.getInt(4));
            assertFalse(rs.next());
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }

    @Test
    public void testCompareAndOrExpression() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into UpsertCompare (k,value3) values (?, ?) compare value1 is null and (value2='root' or value3=1)");
            stmt.setString(1, "Key-1");
            stmt.setInt(2, 5);
            stmt.executeUpdate();
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select * from UpsertCompare");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Key-1", rs.getString(1));
            assertEquals(null, rs.getString(2));
            assertEquals("Compare Test", rs.getString(3));
            assertEquals(5, rs.getInt(4));
            assertFalse(rs.next());
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }

    @Test
    public void testCompareLikeExpression() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into UpsertCompare (k,value3) values (?, ?) compare value2 like 'Compare%'");
            stmt.setString(1, "Key-1");
            stmt.setInt(2, 5);
            stmt.executeUpdate();
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select * from UpsertCompare");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Key-1", rs.getString(1));
            assertEquals(null, rs.getString(2));
            assertEquals("Compare Test", rs.getString(3));
            assertEquals(5, rs.getInt(4));
            assertFalse(rs.next());
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }

    @Test
    public void testCompareRowKey() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into UpsertCompare (k,value3) values (?, ?) compare k='Key-1'");
            stmt.setString(1, "Key-1");
            stmt.setInt(2, 5);
            stmt.executeUpdate();
            conn.commit();
        } catch (SQLException ex) {
            assertEquals("Use of row key in compare clause in currently not supported", ex.getMessage());
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }

    @Test public void testCompareMultipleRowUpdates() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into UpsertCompare (k, value3) values (?, ?) compare value3=1");
            stmt.setString(1, "Key-1");
            stmt.setInt(2, 5);
            stmt.executeUpdate();
            stmt = conn.prepareStatement("upsert into UpsertCompare (k, value3) values (?, ?) compare value3=5");
            stmt.setString(1, "Key-1");
            stmt.setInt(2, 9);
            stmt.executeUpdate();
            stmt = conn.prepareStatement("upsert into UpsertCompare (k, value3) values (?, ?) compare value3=9");
            stmt.setString(1, "Key-1");
            stmt.setInt(2, 11);
            stmt.executeUpdate();
            stmt = conn.prepareStatement("upsert into UpsertCompare (k, value3) values (?, ?) compare value3=11");
            stmt.setString(1, "Key-1");
            stmt.setInt(2, 64);
            stmt.executeUpdate();
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select * from UpsertCompare");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Key-1", rs.getString(1));
            assertEquals(null, rs.getString(2));
            assertEquals("Compare Test", rs.getString(3));
            assertEquals(64, rs.getInt(4));
            assertFalse(rs.next());
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }

    @Test public void testUpsertIfNotExist() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = null;
        PreparedStatement stmt = null;

        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("upsert into UpsertCompare (k, value3) values (?, ?) compare value3 is null");
            stmt.setString(1, "Key-abc");
            stmt.setInt(2, 5);
            stmt.executeUpdate();
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            stmt = conn.prepareStatement("select * from UpsertCompare where k=?");
            stmt.setString(1, "Key-abc");
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Key-abc", rs.getString(1));
            assertEquals(null, rs.getString(2));
            assertEquals(null, rs.getString(3));
            assertEquals(5, rs.getInt(4));
            assertFalse(rs.next());
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }
}
