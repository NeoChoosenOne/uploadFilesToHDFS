package com.cbds.uploadfiles.hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;


//Class to upload files from a local file system (windows, linux, etc) to HDFS
public class UploadFilesHDFS {
	public static void main(String [] args){
		try {
			//Path into HDFS wehere the local files going to load
			String hdfsPath				= "/linearregresion/";
			//Instance of this class
			UploadFilesHDFS upload 		= new UploadFilesHDFS();
			//call the method from upload object to get the hadoop configuration
			Configuration configuration = upload.getConfiguration();
			//FileWriter object this two lines are two make a file that will store all the name of the files that we upload into HDFS. 
			FileWriter fw 	  = new FileWriter(args[0] + "/names",true);
			BufferedWriter bw = new BufferedWriter(fw);
		
			//Conditional statement that checks if hadoop configuration is not null and if the HDFS path was created successfully.
			if(configuration!=null && upload.createStructured(configuration,hdfsPath)){
				//Path to the folder into the local file system where the files stays
				File dfbFiles = new File(args[0]);
				//Loop to go over the files into folder excepts file with name names"			
				for(File file : dfbFiles.listFiles()){
					if(!file.getName().equalsIgnoreCase("names")){
						//method to upload a local file system file into the path of HDFS
						upload.localfilesystemToHDFS(file, configuration,hdfsPath + args[0],bw);
					}
				}
			}
			//If bw is different of null close it
			if (bw != null)
				bw.close();
			//If fw is different of null close it
			if (fw != null)
				fw.close();
			
			//Load file with name "names" into HDFS structure
			upload.localfilesystemToHDFS(new File(args[0] + "/names"), configuration,hdfsPath + "aux/",null);
			
		} catch (IOException e) {
			System.out.println("El error es: " + e.getMessage());
		}
	}
	
	//Method to return a hadoop configuration
	public Configuration getConfiguration(){
		try{
			//Instance of a hadoop configuration 
			Configuration conf = new Configuration();
			//Property to hadoop configuration to point to HDFS cluster
			conf.set("fs.default.name", "hdfs://localhost:9000");
			//Property to hadoop configuration to point to user who will control the next steps
			conf.set("hadoop.job.ugi","cluster");
			//Properties that are necessaries to make reference to each META-INFO/services/org.apache.hadoop.fs.FileSystem
			//Property to hadoop configuration to tell it where the DistributedFileSystem class
			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			//Property to hadoop configuration to tell it where the LocalFileSystem class
			conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
			return conf;
		}catch(Exception e){
			System.out.println("El error es el siguiente: " + e.getMessage());
			return null;
		}
	}
	
	//Method to create a Path on HDFS.
	public boolean createStructured(Configuration conf,String hdfsPath){
		//Defined a FileSystem Object
		FileSystem fs;
		try {
			//Instance a FileSystem Object getting from the configuration of the previus steps
			fs = FileSystem.get(conf);
			//Checks if not exist the path into HDFS.
			if(!fs.exists(new Path(hdfsPath))){
				//method to create a path into HDFS with permission 755
		        fs.mkdirs(new Path(hdfsPath),new FsPermission((short)0755));
		        //Close the FileSystem Instance
		        fs.close();
			}
			return true;
		} catch (IOException e) {
			System.out.println("El error es el siguiente: " + e.getMessage());
			return false;
		}
	}
	
	//Method to upload local file system into HDFS.
	public boolean localfilesystemToHDFS(File file, Configuration conf,String hdfsPath,BufferedWriter bw){
		try{
			 String filename = file.getName();
			 System.out.println("Subiendo a HDFS el archivo: " + filename);
			 //FileSystem object that need a hadoop configuration to make basic operations on HDFS 
			 FileSystem fs = FileSystem.get(conf);	
			 //Path where the local files going to store on HDFS
	         Path fileDestino = new Path(hdfsPath + "/" + filename);
	         //Object to get a output stream object to HDFS to create a file into it
	         FSDataOutputStream out = fs.create(fileDestino);
	         //Read the local file and get the input stream of it
	         InputStream in = new BufferedInputStream(new FileInputStream(new File(file.getAbsolutePath())));	
	         byte[] b = new byte[1024];
	         int numBytes = 0;
	         //Writes bytes into inputo stream to ouput stream
	         while ((numBytes = in.read(b)) > 0) {
	           out.write(b, 0, numBytes);
	         }
	         //Write into a file the name of each file that is store on HDFS.
	         if(bw!=null){
		         bw.write(filename + "\n");
	         }
	         //Close input stream object
	         in.close();
	         //Close output stream object
	         out.close();
	 		return true;
		}catch(Exception e){
			System.out.println("Ocurrio un error con el archivo " + file.getName() + ":" + e.getMessage());
			return false;
		}
	}
}
