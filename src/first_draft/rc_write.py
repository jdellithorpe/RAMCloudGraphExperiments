import sys
import ramcloud

GRAPH_TABLE_NAME = "graph"
COORDINATOR_LOCATION = "infrc:host=192.168.1.101,port=12246"

def main():
    # get connected to ramcloud
    rc = ramcloud.RAMCloud()
    rc.connect(COORDINATOR_LOCATION)

    graph_tableid = rc.get_table_id(GRAPH_TABLE_NAME)
  
    key = sys.argv[1]
    value = sys.argv[2]
    rc.write(graph_tableid, key, value)

if __name__ == '__main__':
    main()
