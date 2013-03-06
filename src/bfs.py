import sys
import Queue
import ramcloud

GRAPH_TABLE_NAME = "graph"
COORDINATOR_LOCATION = "fast+udp:host=127.0.0.1,port=12246"

def main():
    # get connected to ramcloud
    rc = ramcloud.RAMCloud()
    rc.connect(COORDINATOR_LOCATION)

    graph_tableid = rc.get_table_id(GRAPH_TABLE_NAME)

    # run breadth first search on the graph    
    source = sys.argv[1]
    queue = Queue.Queue()
    queue.put(source)
    shortest_path = {}
    shortest_path[source] = (source, 0)
    while not queue.empty():
        u = queue.get()
        path_to_u, dist_to_u = shortest_path[u]
        
        try:
            value, version = rc.read(graph_tableid, u)
            for v in value.split(' '):
                if v != "-1":
                    try:
                        shortest_path[v]
                    except:
                        queue.put(v)
                        shortest_path[v] = (path_to_u + "->" + v, dist_to_u + 1)
                else:
                    rc.write(graph_tableid, u, value[0:-2] + path_to_u + ' ' + str(dist_to_u))
        except:
            # this node is a leaf                   
            rc.write(graph_tableid, u, path_to_u + ' ' + str(dist_to_u))

if __name__ == '__main__':
    main()
