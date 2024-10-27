#
# Main program for photoapp program using AWS S3 and RDS to
# implement a simple photo application for photo storage and
# viewing.
#
# Authors:
#   Katerina Falkner
#   Prof. Joe Hummel (initial template)
#   Northwestern University
#

import datatier  # MySQL database access
import awsutil  # helper functions for AWS
import boto3  # Amazon AWS

import uuid
import pathlib
import logging
import sys
import os

from configparser import ConfigParser

import matplotlib.pyplot as plt
import matplotlib.image as img


###################################################################
#
# prompt
#
def prompt():
  """
  Prompts the user and returns the command number
  
  Parameters
  ----------
  None
  
  Returns
  -------
  Command number entered by user (0, 1, 2, ...)
  """

  try:
    print()
    print(">> Enter a command:")
    print("   0 => end")
    print("   1 => stats")
    print("   2 => users")
    print("   3 => assets")
    print("   4 => download")
    print("   5 => download and display")
    print("   6 => upload")
    print("   7 => add user")

    cmd = int(input())
    return cmd

  except Exception as e:
    print("ERROR")
    print("ERROR: invalid input")
    print("ERROR")
    return -1


###################################################################
#
# stats
#
def stats(bucketname, bucket, endpoint, dbConn):
  """
  Prints out S3 and RDS info: bucket name, # of assets, RDS 
  endpoint, and # of users and assets in the database
  
  Parameters
  ----------
  bucketname: S3 bucket name,
  bucket: S3 boto bucket object,
  endpoint: RDS machine name,
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """
  #
  # bucket info:
  #
  try: 
    print("S3 bucket name:", bucketname)

    assets = bucket.objects.all()
    print("S3 assets:", len(list(assets)))

    #
    # MySQL info:
    #
    print("RDS MySQL endpoint:", endpoint)

    sql = """
    select count(userid) from users;
    """

    row = datatier.retrieve_one_row(dbConn, sql)
    if row is None:
      print("Database operation failed...")
    elif row == ():
      print("Unexpected query failure...")
    else:
      print("# of users: ", row[0])


    sql = """
    select count(assetid) from assets;
    """

    row = datatier.retrieve_one_row(dbConn, sql)
    if row is None:
      print("Database operation failed...")
    elif row == ():
      print("Unexpected query failure...")
    else:
      print("# of assets: ", row[0])


  except Exception as e:
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))

###################################################################
#
# users
#
def users(dbConn):
  """
  Prints out users in descending order by user id
  
  Parameters
  ----------
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """
  #
  # bucket info:
  #
  try: 
    sql = """
    select * from users
    order by userid DESC;
    """

    rows = datatier.retrieve_all_rows(dbConn, sql)
    if rows is None:
      print("Database operation failed...")
    elif rows == ():
      print("Unexpected query failure...")
    else:
      for row in rows:
        print("User id:", row[0])
        print("  Email:", row[1])
        print("  Name:", row[2], ",", row[3])
        print("  Folder:", row[4])

  except Exception as e:
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))


###################################################################
#
# users
#
def assets(dbConn):
  """
  Prints out assets in descending order by user id
  
  Parameters
  ----------
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """
  #
  # bucket info:
  #
  try: 
    sql = """
    select * from assets
    order by assetid DESC;
    """

    rows = datatier.retrieve_all_rows(dbConn, sql)
    if rows is None:
      print("Database operation failed...")
    elif rows == ():
      print("Unexpected query failure...")
    else:
      for row in rows:
        print("Asset id:", row[0])
        print("  User id:", row[1])
        print("  Original name:", row[2])
        print("  Key Name:", row[3])

  except Exception as e:
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))


###################################################################
#
# download
#
def download(bucket, dbConn, display):
  """
  Finds asset in database, downloads it, and renames it to its original filename
  
  Parameters
  ----------
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """
  #
  # bucket info:
  #
  try: 
    asset_id = input("Enter asset id>\n")

    sql = """
      SELECT * FROM users
      INNER JOIN assets on users.userid = assets.userid
      WHERE assets.assetid= %s;    
    """

    row = datatier.retrieve_one_row(dbConn, sql, [asset_id])
    if row is None:
      print("Database operation failed...")
    elif row == ():
      print("No such asset...")
    else:
      key = row[8]
      name = awsutil.download_file(bucket, key)
      if name is not None:
        os.rename(name, row[7])
        print("Downloaded from S3 and saved as '", row[7], "'")
      else:
        print("Cannot find file...")
        sys.exit()
    
    if display == True:
      image = img.imread(row[7])
      plt.imshow(image)
      plt.show()

  except Exception as e:
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))


###################################################################
#
# upload
#
def upload(dbConn, bucket):
  """
  Uploads a file to a user's folder in S3
  
  Parameters
  ----------
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """
  #
  # bucket info:
  #
  try: 

    filename = input("Enter local filename>\n")
    if not os.path.exists(filename):
      print("Local file '", filename, "' does not exist...")
      sys.exit()

    userid = input("Enter user id>\n")
    sql = """
      select * from users 
      where userid = %s;
      """
    row = datatier.retrieve_one_row(dbConn, sql, [int(userid)])
    if row is None:
      print("Database operation failed...")
    elif row == ():
      print("No such user...")
      sys.exit()

    randuuid = str(uuid.uuid4())
    name = row[4] + "/" + randuuid + ".jpg"
    sql = """
      INSERT INTO 
        assets(userid, assetname, bucketkey)
        values(%s, %s, %s);
    """
    rows = datatier.perform_action(dbConn, sql, [userid, filename, name])
    if rows == -1:
      print("Database operation failed...")
    elif rows == 0:
      print("File was not downloaded...")
    else:
      temp = awsutil.upload_file(filename, bucket, name)
      if temp is not None:
        print("Downloaded from S3 and saved as '", name, "'")
      else:
        print("Could not upload file...")

    sql = """
      SELECT LAST_INSERT_ID();
      """
    row = datatier.retrieve_one_row(dbConn, sql)
    if row is None:
      print("Database operation failed...")
    elif row == ():
      print("No id found...")
    else:
      print("Recorded in RDS under asset id", row[0])

  except Exception as e:
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))


###################################################################
#
# add user
#
def add_user(dbConn):
  """
  Adds a new user to the users table
  
  Parameters
  ----------
  dbConn: open connection to MySQL server
  
  Returns
  -------
  nothing
  """
  #
  # bucket info:
  #
  try: 

    email = input("Enter user's email>\n")
    lastname = input("Enter user's last (family) name>\n")
    firstname = input("Enter user's first (given) name>\n")

    folder = str(uuid.uuid4())
    sql = """
      INSERT INTO 
        users(email, lastname, firstname, bucketfolder)
        values(%s, %s, %s, %s);
    """
    print(email, lastname, firstname, folder)
    rows = datatier.perform_action(dbConn, sql, [email, lastname, firstname, folder])
    if rows == -1:
      print("Database operation failed...")
    elif rows == 0:
      print("User was not created...")

    sql = """
      SELECT LAST_INSERT_ID();
      """
    row = datatier.retrieve_one_row(dbConn, sql)
    if row is None:
      print("Database operation failed...")
    elif row == ():
      print("No id found...")
    else:
      print("Recorded in RDS under user id", row[0])

  except Exception as e:
    print("ERROR")
    print("ERROR: an exception was raised and caught")
    print("ERROR")
    print("MESSAGE:", str(e))


#########################################################################
# main
#
print('** Welcome to PhotoApp **')
print()

# eliminate traceback so we just get error message:
sys.tracebacklimit = 0

#
# what config file should we use for this session?
#
config_file = 'photoapp-config.ini'

print("What config file to use for this session?")
print("Press ENTER to use default (photoapp-config.ini),")
print("otherwise enter name of config file>")
s = input()

if s == "":  # use default
  pass  # already set
else:
  config_file = s

#
# does config file exist?
#
if not pathlib.Path(config_file).is_file():
  print("**ERROR: config file '", config_file, "' does not exist, exiting")
  sys.exit(0)

#
# gain access to our S3 bucket:
#
s3_profile = 's3readwrite'

os.environ['AWS_SHARED_CREDENTIALS_FILE'] = config_file

boto3.setup_default_session(profile_name=s3_profile)

configur = ConfigParser()
configur.read(config_file)
bucketname = configur.get('s3', 'bucket_name')

s3 = boto3.resource('s3')
bucket = s3.Bucket(bucketname)

#
# now let's connect to our RDS MySQL server:
#
endpoint = configur.get('rds', 'endpoint')
portnum = int(configur.get('rds', 'port_number'))
username = configur.get('rds', 'user_name')
pwd = configur.get('rds', 'user_pwd')
dbname = configur.get('rds', 'db_name')

dbConn = datatier.get_dbConn(endpoint, portnum, username, pwd, dbname)

if dbConn is None:
  print('**ERROR: unable to connect to database, exiting')
  sys.exit(0)

#
# main processing loop:
#
cmd = prompt()

while cmd != 0:
  #
  if cmd == 1:
    stats(bucketname, bucket, endpoint, dbConn)
  elif cmd == 2:
    users(dbConn)
  elif cmd == 3:
    assets(dbConn)
  elif cmd == 4:
    download(bucket, dbConn, False)
  elif cmd == 5:
    download(bucket, dbConn, True)
  elif cmd == 6:
    upload(dbConn, bucket)
  elif cmd == 7:
    add_user(dbConn)
  else:
    print("** Unknown command, try again...")
  #
  cmd = prompt()

#
# done
#
print()
print('** done **')
