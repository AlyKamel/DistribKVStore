package de.tum.i13.server.kv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.logging.Logger;
import de.tum.i13.server.threadperconnection.Main;

/**
 * Persists KV-pairs on disk at the path specified.
 */
public class DiskStore implements KVStorageSystem {

  // constants
  private final static Logger logger = Logger.getLogger(Main.class.getName());
  private final String dataPath; // the path to the base folder which holds the 3 sub folders
  private final char euro = (char) 8364; // euro character

  // attributes
  private int lookID; // defines in which folder we currently look can be 0, 1 or 2
  private String lookPath;
  
  /**
   * Constructor of the class. It creates 3 new sub folders in the directory of dataPath. Each of
   * them represents either the original data of this server or one of the the two replicas.
   * @param dataPath the path to the folder its allowed to work on
   */
  public DiskStore(String dataPath) {
  	this.dataPath = dataPath;
  	
    // create 3 folders, 1 for the original data, the first and second replica
    new File(dataPath.toString() + "/0").mkdirs(); // replica 0 = original
    new File(dataPath.toString() + "/1").mkdirs(); // replica 1
    new File(dataPath.toString() + "/2").mkdirs(); // replica 2
    
    new File(dataPath.toString() + "/chats").mkdirs();
    
    lookPath = dataPath + "/0"; //default value
    lookID = 0;
  }

  /**
   * Creates or overrides a .txt file which represents the KV-pair. Always call setLookID first
   * before calling this method, to make sure you are working on the correct directory.
   * 
   * @param key key of the KV-pair
   * @param value value of the KV-pair
   * @return info about success, error or override happened
   */
  public ServerStatus put(String key, String value) {
    String keyPath = keyPath(key);
    File f = new File(keyPath);
    boolean update = f.exists();
    ServerStatus status;

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(f))) {
      if (update) {
        logger.finer("Overwriting file of KEY " + key);
        status = ServerStatus.UPDATE;
      } else {
        logger.finer("Creating new file for KEY " + key);
        status = ServerStatus.SUCCESS;
      }
      f.createNewFile();
      writer.write(value);
    } catch (IOException e) {
      logger.severe("Error: Failed at creating/writing txt file");
      status = ServerStatus.ERROR;
    }

    return status;
  }

  /**
   * Opens the corresponding .txt file for the parameter key. Always call setLookID first before 
   * calling this method, to make sure you are working on the correct directory.
   * 
   * @param key key of the kv-pair
   * @return the first line in the .txt file = value, on failure it return null
   */
  public String get(String key) {
    String keyPath = keyPath(key);
    try (BufferedReader reader = new BufferedReader(new FileReader(keyPath))) {
      return reader.readLine();
    } catch (IOException e) {
      return null; // key not found
    }
  }

  /**
   * Deletes the .txt file associated with the key in the directory defined in lookPath. If .txt
   * doesn't exist it does nothing. Always call setLookID first before calling this method, to make
   * sure you are working on the correct directory.
   * 
   * @param key key of the KV-pair
   * @return ServerStatus.SUCCESS on successful deletion, else ServerStatus.ERROR
   */
  public ServerStatus delete(String key) {
    String keyPath = keyPath(key);
    File f = new File(keyPath);
    if (!f.exists()) {
      logger.finer(key + " not found");
    } else if (f.delete()) {
      logger.finest("Deletion success with KEY : " + key);
      return ServerStatus.SUCCESS;
    } else {
      logger.severe("Deletion failure with KEY : " + key);
    }
    return ServerStatus.ERROR;
  }

  /**
   * Some keys are invalid as file names and have to be translated into a valid file name. e.g.
   * specific names like "AUX" are forbidden or names starting with a '.' etc.
   * 
   * @param key to be translated key
   * @return corresponding file name for the key
   */
  private String keyToFileName(String key) {
    StringBuilder fileName = new StringBuilder(41);
    for (int i = 0; i < key.length(); i++) {
      char c = key.charAt(i);
      if (isUnusableChar(c)) {
        fileName.append(euro);
        fileName.append((char) (c + 130));
      } else {
        fileName.append(c);
      }
    }
    fileName.append(euro);
    return fileName.toString();
  }

  /**
   * Reverts the effects of the keyToFileName(key) function
   * 
   * @param fileName to be translated file name
   * @return corresponding key for the file name
   */
  private String fileNameToKey(String fileName) {
    StringBuilder key = new StringBuilder(20);
    for (int i = 0; i < fileName.length() - 5; i++) { // we ignore the end (".txt")
      char c = fileName.charAt(i);
      if (c == euro) { // check if it needs to be translated
        key.append((char) (fileName.charAt(++i) - 130));
      } else {
        key.append(c);
      }
    }
    return key.toString();
  }

  /**
   * Checks if character is not allowed in a file name.
   * 
   * @param c
   * @return true if an operating system does not allow the use of the character in a file name
   */
  private boolean isUnusableChar(char c) {
    return c == '\"' || c == '<' || c == '>' || c == ':' || c == '/' || c == '\\' || c == '|'
        || c == '?' || c == '*' || c == '.';
  }
  
  /**
   * Defines on which folder you are currently working on. Set lookID depending on the folder:
   * 0 for original, 1 for replica 1, 2 for replica 2. On success it changes lookID and lookPath
   * of the object, which are crucial for almost every other method in the class. Throws error on
   * failure.
   * */
  public void setLookID(int lookID) {
  	if(this.lookID == lookID) {
  		return;
  	}
  	else if (lookID >= 3 || lookID < 0) {
    	logger.severe("Error @setlookID: bad input, lookID = " + lookID);
    } else {
    	this.lookID = lookID;
	    lookPath = dataPath + "/" + lookID; 
	    logger.finest("Success @setLookID: lookID = " + lookID);
	  }
  }
  
  /**
   * Returns the value of the variable lookID. It can either be 0, 1 or 2 representing the folder
   * that is currently worked on. 0 for original, 1 for replica 1, 2 for replica 2.
   * 
   * @return lookID value of this object
   * */
  public int getLookID() {
    return this.lookID;
  } 
  
  /**
   * Returns an array of all the keys in the directory defined by lookPath. Always call setLookID
   * first before calling this method, to make sure you are working on the correct directory.
   * 
   * @return an array of all the keys in the directory
   */
  public String[] getAllKeys() {
    File[] files = getAllTxtFiles();
    String[] keys = new String[files.length];
    for (int i = 0; i < keys.length; i++) {
      String fileName = files[i].getName();
      keys[i] = fileNameToKey(fileName);
    }
    return keys;
  }

  /**
   * Deletes the base folder. This includes all data and sub directories.
   * 
   * @return true if the deletion process has been successful, false otherwise
   */
  public boolean deleteAll() {
  	if(deleteFolder(dataPath)) {
  		return true;
  	} else {
  		logger.severe("Error @deleteAll files might not be deleted");
  		return false;
  	}  
  }
  
  /**
   * Deletes every file in the directory defined by lookPath. Always call setLookID first before 
   * calling this method, to make sure you are working on the correct directory.
   * 
   * @return true if the deletion process has been successful, false otherwise
   * */
  public boolean emptySubFolder() {
		if(deleteFolder(lookPath)) {
			new File(dataPath.toString() + "/" + lookID).mkdirs();
			return true;
		} else {
			logger.warning("Error @emptySubFolder: files might not be deleted, lookID = " + lookID);
			return false;
		}
  }
  
  /**
   * Deletes everything in the directory and the directory itself. Also deletes inner folders 
   * recursively.
   * 
   * @param path path to the to be deleted directory
   * @return true if the deletion process has been successful, false otherwise
   * */
  private boolean deleteFolder(String path) {
    File folder = new File(path);
    File[] files = folder.listFiles();
    if (files != null) {
      for (File f : files) {
        deleteFolder(f.getPath());  
      } 
    }
    return folder.delete();
  }
  
  /**
   * The destination directory gets replaced by the source directory. After that the source
   * directory is empty.
   * 
   * @param src ID of the source directory
   * @param dst ID of the destination directory
   * @return true on success, false when a error occurred
   * */
  public boolean replaceFolder(int src, int dst) {
  	if(src == dst || src < 0 || src > 2 || dst < 0 || dst > 2) {
  		logger.warning("Error: @replaceFolder src = " + src + " dst = " + dst);
  		return false;
  	}
  	
  	//delete destination folder
  	setLookID(dst);
  	if(!deleteFolder(lookPath))
  		return false;
  	
  	//rename source folder to destination folder
  	File dstFile = new File(lookPath);
  	setLookID(src);
  	File srcFile = new File(lookPath);
  	if(!srcFile.renameTo(dstFile)) {
  		logger.severe("Error: @replaceFolder rename");
  		return false;
  	}

  	//create empty source folder
  	new File(dataPath.toString() + "/" + src).mkdirs();
  	logger.finest("Success @replaceFolder src = " + src + " dst = " + dst);
  	return true;
  }
  
  /**
   * Copies all files in source directory into the destination directory.
   * 
   * @param src ID of the source directory
   * @param dst ID of the destination directory
   * */
  public void copyFolder(int src, int dst) {
  	String srcPath = dataPath + "/" + src;
  	String dstPath = dataPath + "/" + dst;
  	
  	setLookID(src);
  	String[] keys = getAllKeys();	
  	for(String k : keys) {
  		lookPath = srcPath;
  		String value = get(k);
  		lookPath = dstPath;
  		put(k, value);
  	}
  	logger.finest("Success @copyFolder src = " + src + " dst = " + dst);
  } 
  
  /**
   * Returns all the .txt files in the directory using a filter. Always call setLookID first before
   * calling this method, to make sure you are working on the correct directory.
   * 
   * @return array of all the .txt files in the directory
   */
  private File[] getAllTxtFiles() {
    FilenameFilter filter = new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".txt"); // filter all files that end with .txt
      }
    };
    File folder = new File(lookPath);
    return folder.listFiles(filter);
  }
  
  /**
   * Returns the file name of the corresponding .txt file for the key
   * 
   * @param key key of the KV-pair
   * @return the path to the .txt as String
   */
  private String keyPath(String key) {
    String fileName = keyToFileName(key);
    return lookPath + "/" + fileName + ".txt";
  }

  public void chatAppend(String chatID, String msg) {
    String chatPath = dataPath + "/chats/" + chatID + ".txt";
    File f = new File(chatPath);
    boolean exists = f.exists();

    try (FileWriter writer = new FileWriter(f, true)) {
      if (!exists) {
        f.createNewFile();
      }
      writer.write(msg + "\r\n");
      writer.flush();
    } catch (IOException e) {
      logger.severe("Error: Failed at creating/writing txt file");
    }


  }
}

