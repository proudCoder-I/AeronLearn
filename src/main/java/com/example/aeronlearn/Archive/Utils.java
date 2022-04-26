package com.example.aeronlearn.Archive;

import java.io.File;
import java.io.IOException;

/**
 * @author : pan.han@okg.com
 * @date : 2022-04-26 10:15
 */
public class Utils {

    public static File createTempDir(){

        final File tempDir;

        try{
            tempDir = File.createTempFile("archive","tmp");
        }catch (IOException ex){
            throw new RuntimeException(ex);
        }

        if (!tempDir.delete()){
            throw new IllegalStateException("Cannot delete tmp file!");
        }

        if (!tempDir.mkdir()){
            throw new IllegalStateException("cannot create folder!");
        }


        return tempDir;
    }
}
