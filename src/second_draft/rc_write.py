import sys
import ramcloud

COORDINATOR_LOCATION = "infrc:host=192.168.1.101,port=12247"

def main():
    # get connected to ramcloud
    rc = ramcloud.RAMCloud()
    rc.connect(COORDINATOR_LOCATION)

    tableid = rc.get_table_id(sys.argv[1])
  
    key = sys.argv[2]
    value = sys.argv[3]
    rc.write(tableid, key, value)

if __name__ == '__main__':
    main()
