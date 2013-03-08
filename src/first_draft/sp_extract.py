import sys
import ramcloud

GRAPH_TABLE_NAME = "graph"
COORDINATOR_LOCATION = "fast+udp:host=127.0.0.1,port=12246"

def main():
    # get connected to ramcloud
    rc = ramcloud.RAMCloud()
    rc.connect(COORDINATOR_LOCATION)

    graph_tableid = rc.get_table_id(GRAPH_TABLE_NAME)
  
    start_key = sys.argv[1]
    end_key = sys.argv[2]
    for nodeid in range(int(start_key), int(end_key) + 1):
      try:
        value, version = rc.read(graph_tableid, str(nodeid))
        if value.split(' ')[-1] != "-1":
          print str(nodeid) + ": " + value.split(' ')[-1]
      except:
        pass

if __name__ == '__main__':
    main()
