import sys
import Queue
import ramcloud

GRAPH_TABLE_NAME = "graph"
COORDINATOR_LOCATION = "fast+udp:host=127.0.0.1,port=12246"

def main():
    # get connected to ramcloud
    rc = ramcloud.RAMCloud()
    rc.connect(COORDINATOR_LOCATION)

    # drop graph table from ramcloud if it's in there
    rc.drop_table(GRAPH_TABLE_NAME)

    # create graph table in ramcloud
    rc.create_table(GRAPH_TABLE_NAME)
    graph_tableid = rc.get_table_id(GRAPH_TABLE_NAME)

    # open graph file
    graph_filename = sys.argv[1]
    graph_file = open(graph_filename, 'r')
    
    # read graph into ramcloud
    graph_edges = graph_file.readlines()
    key = ""
    value = ""
    for edge in graph_edges:
        edge_src, edge_dst = edge.strip('\n').split(' ')
        if( edge_src == key ):
            value = value + edge_dst + ' '
        else:
            if( key ):
                rc.write(graph_tableid, key, value + "-1")
            key = edge_src
            value = edge_dst + ' '
    if( key ):
        rc.write(graph_tableid, key, value + "-1")

    # run breadth first search on the graph    
    source = sys.argv[2]
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
                    rc.write(graph_tableid, u, value[0:-2] + str(dist_to_u) + ' ' + path_to_u)
        except:
            # this node is a leaf                   
            rc.write(graph_tableid, u, str(dist_to_u) + ' ' + path_to_u)

    # show us the graph
    #for i in range(1,4038):
    #    try:
    #        print str(i) + ": ",
    #        print rc.read(graph_tableid, str(i)) 
    #    except:
    #        print " "

    # drop graph table from ramcloud
    rc.drop_table(GRAPH_TABLE_NAME)

if __name__ == '__main__':
    main()
