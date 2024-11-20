package org.example;

import example.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;

public class Main {
    public static final String USERS_AVRO = "users.avro";

    public static void main(String[] args) {
        final File usersAvroFile = new File(USERS_AVRO);
        if (usersAvroFile.exists()) {
            System.out.println("Reading users from disk");
            DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
            try (DataFileReader<User> dataFileReader = new DataFileReader<User>(usersAvroFile, userDatumReader)) {
                User user = null;
                while (dataFileReader.hasNext()) {
                    user = dataFileReader.next(user);
                    System.out.println(user);
                }
            } catch (Exception exception) {
                System.err.println("Unable to retrieve users from disk: " + exception.getMessage());
            }
            System.out.println("Finished reading users from disk");
        } else {
            System.out.println("Writing users to disk");
            // Serialize user1, user2 and user3 to disk
            DatumWriter<User> userDatumWriter = new SpecificDatumWriter<>(User.class);
            try (DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userDatumWriter)) {
                User user1 = new User();
                user1.setName("Alyssa");
                user1.setFavoriteNumber(256);

//                User user2 = new User("Ben", 7, "red");

                User user3 = User.newBuilder()
                        .setName("Charlie")
                        .setFavoriteColor("blue")
                        .setFavoriteNumber(null)
                        .build();
                dataFileWriter.create(user1.getSchema(), new File(USERS_AVRO));
                dataFileWriter.append(user1);
//                dataFileWriter.append(user2);
                dataFileWriter.append(user3);
            } catch (Exception exception) {
                System.err.println("Unable to write users to disk: " + exception.getMessage());
            }
            System.out.println("Finished writing users to disk");
        }
    }
}