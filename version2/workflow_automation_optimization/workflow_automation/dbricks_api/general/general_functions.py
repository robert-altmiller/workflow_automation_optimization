# Databricks notebook source
# DBTITLE 1,Create an MD5 Hash out of a Python String
def str_to_md5_hash(inputstr = None):
  """encode string using md5 hash"""
  return hashlib.md5(inputstr.encode())

# COMMAND ----------

# DBTITLE 1,Create Databricks File System Folder
def create_dbfs_folder(folderpath = None):
  """create databricks file system folder"""
  try:
    result = dbutils.fs.mkdirs(folderpath)
    return f"{folderpath} created successfully...."
  except: return f"{folderpath} could not be created...."

# COMMAND ----------

# DBTITLE 1,Create a New File in Databricks DBFS
def create_dbfs_new_file(folderpath = None, filename = None, overwrite = True):
  """create a new file in databricks file system"""
  try:
    filepath = f"{folderpath}/{filename}"
    result = dbutils.fs.put(filepath, "", overwrite)
    return f"{filepath} created successfully...."
  except: return f"{filepath} could not be created...."

# COMMAND ----------

# DBTITLE 1,Move File Into Databricks DBFS

def copy_file_into_dbfs(currentfilepath = None, dbfsnewfolderpath = None, overwrite = True):
  """move a local file in databricks into databricks file system"""
  try:
    result = create_dbfs_folder(dbfsnewfolderpath)
    currfolderpath = currentfilepath.rsplit("/", 1)[0]
    currfilename = currentfilepath.rsplit("/", 1)[1]
    dbfsnewfilepath = f"{dbfsnewfolderpath}/{currfilename}"
    dbutils.fs.cp(currentfilepath, dbfsnewfilepath, overwrite)
    return print(f"{currentfilepath} copied to {dbfsnewfilepath} successfully.....")
  except:
    return f"{currentfilepath} could not be copied to {dbfsnewfilepath}....."

# COMMAND ----------

# DBTITLE 1,Delete Databricks File System Folder
def delete_dbfs_folder(folderpath = None):
  """delete databricks file system folder"""
  try:
    result = dbutils.fs.rm(folderpath)
    return f"{folderpath} removed successfully...."
  except: return f"{folderpath} could not be removed...."

# COMMAND ----------

# DBTITLE 1,Get Databricks File System File Name
def get_dbfs_file_name(dbfsfilepath = None, file_ext = None):
  """get a dbfs file name"""
  files = dbutils.fs.ls(dbfsfilepath)
  for file in files:
    if file_ext in file[1]: return file[1]
  else: return None

# COMMAND ----------

# DBTITLE 1,Remove a Substring From a String Input
def check_str_for_substr_and_replace(inputstr = None, substr = None):
    """remove a substring from a string input"""
    if substr in inputstr:
        return inputstr.replace(substr, '')
    else: return inputstr

# COMMAND ----------

# DBTITLE 1,Url Encoding HTML String Function
def url_encode_str(inputstr = None):
  """url encode an input string"""
  return urllib.parse.quote_plus(inputstr)

# COMMAND ----------

# DBTITLE 1,Create a Directory Using OS
def os_create_dir(folderpath = None, ignore_errors = True):
  """create a local directory using OS"""
  shutil.rmtree(folderpath, ignore_errors = ignore_errors, onerror = None)
  os.makedirs(folderpath)

# COMMAND ----------

# DBTITLE 1,Write a File to DBFS
def write_new_file(folderpath = None, filename = None, data = None, writemethod = "a"):
  """write a new file to databricks file system"""
  try:
    filepath = f"{folderpath}/{filename}"
    f = open(filepath, writemethod) # params: a = append, w = write
    f.write(data)  
    f.close()
    return f"filpath '{filepath}' written successfully..."
  except PermissionError:
      return f"[Errno 13] Permission denied for path '{filepath}'"
