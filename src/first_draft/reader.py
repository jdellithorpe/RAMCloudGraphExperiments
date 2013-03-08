import sys
import ramcloud

GRAPH_TABLE_NAME = "graph"
COORDINATOR_LOCATION = "infrc:host=192.168.1.101,port=12246"

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
    
    # get source node
    src_node = sys.argv[2]

    # read graph into ramcloud
    graph_edges = graph_file.readlines()
    key = ""
    value = ""
    for edge in graph_edges:
        edge_src, edge_dst = edge.strip('\n').split('\t')
        if( edge_src == key ):
            value = value + edge_dst + ' '
        else:
            if( key ):
              if( key == src_node ):
                rc.write(graph_tableid, key, value + "0")
              else:
                rc.write(graph_tableid, key, value + "-1")
            key = edge_src
            value = edge_dst + ' '
    if( key ):
        if( key == src_node ):
          rc.write(graph_tableid, key, value + "0")
        else:
          rc.write(graph_tableid, key, value + "-1")

if __name__ == '__main__':
    main()
