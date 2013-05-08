#ifndef SANDY_SANDY_H
#define SANDY_SANDY_H

#define GRAPH_TABLE_NAME "graph"
#define DIST_TABLE_NAME "dist"
#define COORDINATOR_LOCATION "infrc:host=192.168.1.101,port=12246"
#define REPORTER_NAP_TIME 10

#define MULTIWRITE_REQ_MAX (size_t)400
#define MULTIREAD_REQ_MAX (size_t)1000

#define FRONTIER_EDGE_Q_SIZE_MAX (size_t)1000000

#define EDGE_BATCH_MAX (size_t)1000

#endif // SANDY_SANDY_H
