package com.infernalsuite.aswm.loaders.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.session.Request;
import com.infernalsuite.aswm.api.exceptions.UnknownWorldException;
import com.infernalsuite.aswm.loaders.UpdatableLoader;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class CassandraLoader extends UpdatableLoader {
    @Override
    public void update() throws NewerDatabaseException, IOException {
    }

    @Override
    public byte[] readWorld(String worldName) throws UnknownWorldException, IOException {
        try (CqlSession session = CqlSession.builder().addContactPoint(
                        new InetSocketAddress("10.8.0.1", 9042))
                .withLocalDatacenter("datacenter1").build()
        ) {
            PreparedStatement pStatement = session.prepare("SELECT data FROM swm.worlds WHERE name=?");
            BoundStatement bStatement = pStatement.bind(worldName);
            ResultSet resultSet = session.execute(bStatement);
            for (Row row : resultSet) {
                if (row == null) {
                    throw new UnknownWorldException("Resulting row is null.");
                }
                return row.getByteBuffer("data").array();
            }
        } catch (Exception e) {
            throw new UnknownWorldException("Unable to retrieve world from db: " + e.getMessage());
        }
        return new byte[0];
    }

    @Override
    public boolean worldExists(String worldName) throws IOException {
        try (CqlSession session = CqlSession.builder().addContactPoint(
                        new InetSocketAddress("10.8.0.1", 9042))
                .withLocalDatacenter("datacenter1").build()
        ) {
            PreparedStatement pStatement = session.prepare("SELECT name FROM swm.worlds WHERE name=?");
            BoundStatement bStatement = pStatement.bind(worldName);
            ResultSet resultSet = session.execute(bStatement);
            for (Row row : resultSet) {
                if (row == null) {
                    throw new UnknownWorldException("Queried world does not exist.");
                } else {
                    return true;
                }
            }
        } catch (Exception e) {
            throw new IOException("Unable to retrieve world from db: " + e.getMessage());
        }
        return false;
    }

    @Override
    public List<String> listWorlds() throws IOException {
        List<String> worldNames = new ArrayList<>();
        try (CqlSession session = CqlSession.builder().addContactPoint(
                        new InetSocketAddress("10.8.0.1", 9042))
                .withLocalDatacenter("datacenter1").build()
        ) {
            PreparedStatement pStatement = session.prepare("SELECT name FROM swm.worlds;");
            ResultSet resultSet = session.execute(pStatement.getQuery());
            for (Row row : resultSet) {
                if (row == null) {
                    throw new UnknownWorldException("Invalid resultset row");
                }
                worldNames.add(row.getString("name"));
            }
        } catch (Exception e) {
            throw new IOException("Unable to retrieve world list from db: " + e.getMessage());
        }
        return worldNames;
    }

    @Override
    public void saveWorld(String worldName, byte[] serializedWorld) throws IOException {
        try (CqlSession session = CqlSession.builder().addContactPoint(
                        new InetSocketAddress("10.8.0.1", 9042))
                .withLocalDatacenter("datacenter1").build()
        ) {
            PreparedStatement pStatement;
            BoundStatement bStatement = null;
            ByteBuffer buffer = ByteBuffer.wrap(serializedWorld);
            pStatement = session.prepare("INSERT INTO swm.worlds (name) VALUES (?) IF NOT EXISTS");
            bStatement = pStatement.bind(worldName);
            session.execute(bStatement);
            pStatement = session.prepare("UPDATE swm.worlds SET data=? WHERE name=?  IF EXISTS");
            bStatement = pStatement.bind(buffer, worldName);
            session.execute(bStatement);
        } catch (Exception e) {
            throw new IOException(e);
        }


    }

    @Override
    public void deleteWorld(String worldName) throws UnknownWorldException, IOException {
        try (CqlSession session = CqlSession.builder().addContactPoint(
                        new InetSocketAddress("10.8.0.1", 9042))
                .withLocalDatacenter("datacenter1").build()
        ) {
            PreparedStatement pStatement;
            BoundStatement bStatement = null;
            pStatement = session.prepare("DELETE * FROM swm.worlds WHERE name=? IF EXISTS");
            bStatement = pStatement.bind(worldName);
            session.execute(bStatement);
        } catch (Exception e) {
            throw new IOException(e);
        }

    }
}
