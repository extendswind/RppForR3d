package com.cug.geo3d.upload;


import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;


public class SpatialMapUploader {

    public SpatialMapUploader() {

    }

    public static void generateTestData(String filename, int sizeX, int sizeY) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(new File(filename));
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));

        for (int i = 0; i < sizeY; i++) {
            for (int j = 0; j < sizeX; j++) {
                bufferedWriter.write(Integer.toString(j + i * sizeX));
                bufferedWriter.write("\n");
            }
        }
        bufferedWriter.flush();
        bufferedWriter.close();
    }

    public void splitSpatialData(String filepath, int fileRow, int fileCol, int row, int col) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(filepath))));
        String filename = FilenameUtils.getName(filepath);
        String uploadFilepath = filepath + "_upload";

        BufferedWriter[] resultWriters = new BufferedWriter[row * col];
        FileUtils.forceMkdir(new File(uploadFilepath));

        for (int i = 0; i < row; i++) {
            for (int j = 0; j < col; j++) {
                FileOutputStream fileOutputStream = new FileOutputStream(new File(uploadFilepath +
                        "/grid_" + filename + "_" + Integer.toString(i) + "_" + Integer.toString(j)));
                resultWriters[i * col + j] = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
            }
        }

        int blockWidth = fileCol / col;
        int blockHeight = fileRow / row;

        String lineText = null;
        for (int i = 0; i < fileRow; i++) {
            for (int j = 0; j < fileCol; j++) {
                lineText = br.readLine();
                int writerid = (i / blockHeight) * col + (j / blockWidth);
                resultWriters[writerid].write(lineText + "\n");
            }
        }

        br.close();
        for (int i = 0; i < row * col; i++) {
            resultWriters[i].flush();
            resultWriters[i].close();
        }


    }


    public static void main(String[] args) throws IOException {


        Configuration conf = new Configuration();
        conf.set("dfs.blocksize", "1048576");
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://hadoopmaster:9000/"), conf);

        String uploadFile = "testfile.dat";
        Path uploadFilePath = new Path(uploadFile);

        Path spatialDataPath = new Path("/user/sparkl/spatial_data");
        if(!hdfs.exists(spatialDataPath)){
            hdfs.mkdirs(spatialDataPath);
        }

//        hdfs.mkdirs(new Path("hdfs://tdh1:9000/" + FilenameUtils.getBaseName(uploadFile)));
//
//        hdfs.delete(new Path("hdfs://tdh1:9000/testData"), true);
//        hdfs.copyFromLocalFile(false, true,
//                new Path("/mnt/data/liuzhipeng/test_data/test3mFile"),
//                new Path("hdfs://tdh1:9000/testData"));


        hdfs.close();

    }

}
