import sys
import math
import ramcloud
import subprocess

GRAPH_TABLE_NAME = "graph"
COORDINATOR_LOCATION = "infrc:host=192.168.1.101,port=12246"
SYNC_VEC_KEY = "sync"
DONE_VEC_KEY = "done"

def main():
    # get connected to ramcloud
    rc = ramcloud.RAMCloud()
    rc.connect(COORDINATOR_LOCATION)

    graph_tableid = rc.get_table_id(GRAPH_TABLE_NAME)

    # process arguments
    src_node = sys.argv[1]
    min_node = int(sys.argv[2])
    max_node = int(sys.argv[3])
    num_slaves = int(sys.argv[4])

    # figure out how many nodes we have in total
    nodes_total = max_node - min_node + 1;

    # create the sync vector (all ones)
    rc.write(graph_tableid, SYNC_VEC_KEY, bin((1<<num_slaves)-1)[2:])

    # create the done vector (all zeros)
    rc.write(graph_tableid, DONE_VEC_KEY, bin(1<<num_slaves)[3:])

    pid_list = list()
    nodes_allocated = 0
    # spin up slave processes remotely
    for i in range(1,num_slaves+1):
      node_allocation = int(math.ceil(float(nodes_total - nodes_allocated)/float(num_slaves - i + 1)))
      start_node = min_node + nodes_allocated
      end_node = start_node + node_allocation - 1
      nodes_allocated += node_allocation    

      print "ssh jdellit@rc" + str(i).zfill(2) + " ~/sandy/src/dbfs_slave " + str(start_node) + " " + str(end_node) + " " + str(i-1)

#      pid_list.append(subprocess.Popen(["ssh", "jdellit@rc01", "~/sandy/src/dbfs_slave " + str(start_node) + " " + str(end_node) + " " + str(i-1) ], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE))

      pid_list.append(subprocess.Popen(["ssh", "jdellit@rc" + str(i).zfill(2), "~/sandy/src/dbfs_slave " + str(start_node) + " " + str(end_node) + " " + str(i-1) ], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE))

    while(1):
      sync_vector, version = rc.read(graph_tableid, SYNC_VEC_KEY)
      print "sync: " + sync_vector + " ",
      if sync_vector == str(0).zfill(num_slaves):
        done_vector, version = rc.read(graph_tableid, DONE_VEC_KEY)
        print "done: " + done_vector
        if done_vector == bin((1<<num_slaves)-1)[2:]:
          print "Terminating..."
          rc.write(graph_tableid, SYNC_VEC_KEY, str(2.0/9.0)[2:2+num_slaves])
          break;
        else:
          rc.write(graph_tableid, SYNC_VEC_KEY, bin((1<<num_slaves)-1)[2:]) 
      else:
        print " "
    
    for i in range(0,num_slaves):
      print "waiting for " + str(i) + "... ",
      subprocess.Popen.wait(pid_list[i])
      print "done"

if __name__ == '__main__':
    main()
