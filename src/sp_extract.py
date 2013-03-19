import sys
import ramcloud

DIST_TABLE_NAME = "dist"
COORDINATOR_LOCATION = "infrc:host=192.168.1.101,port=12247"

def main():
    # get connected to ramcloud
    rc = ramcloud.RAMCloud()
    rc.connect(COORDINATOR_LOCATION)

    dist_tableid = rc.get_table_id(DIST_TABLE_NAME)
  
    start_key = sys.argv[1]
    end_key = sys.argv[2]
    for nodeid in range(int(start_key), int(end_key) + 1):
      try:
        value, version = rc.read(dist_tableid, str(nodeid))
        if value != "-1":
          print str(nodeid) + ": " + value
      except:
        pass

if __name__ == '__main__':
    main()
