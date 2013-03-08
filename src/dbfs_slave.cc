#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <string>
#include "RamCloud.h"

#define GRAPH_TABLE_NAME "graph"
#define COORDINATOR_LOCATION "infrc:host=192.168.1.101,port=12246"
#define SYNC_VEC_KEY "sync"
#define DONE_VEC_KEY "done"

using namespace RAMCloud;

int main(int argc, char* argv[]) { 
  // create ramcloud client and connect
  RamCloud client(COORDINATOR_LOCATION);
 
  // get the graph table id
  uint64_t graph_tableid = client.getTableId(GRAPH_TABLE_NAME);

  // get starting node, ending node, and our id
  uint64_t start_node = boost::lexical_cast<int>(argv[1]);
  uint64_t end_node = boost::lexical_cast<int>(argv[2]);
  uint64_t id = boost::lexical_cast<int>(argv[3]);

  // start poll <-> compute loop
  Buffer rc_read_buf;
  while(1) {
    // poll on our synchronization bit
    // when set to "1", we're clear for compute
    // when set to "2", we're clear for termination
    while(1) {
      try {
        client.read(graph_tableid, SYNC_VEC_KEY, sizeof(SYNC_VEC_KEY)-1, &rc_read_buf);
      } catch (RAMCloud::ClientException& e) {
        // what to do here?
        // if the SYNC vector doesn't exist then we should terminate
        return 0;
      }

      uint32_t length = rc_read_buf.getTotalLength();
      string val(static_cast<const char*>(rc_read_buf.getRange(0, length)), length); 
      
      if( boost::lexical_cast<int>(val[id]) == 2 ) {
        // terminate
        return 0;
      } else if( boost::lexical_cast<int>(val[id]) == 1 ) {
        // compute
        break;
      } else {
        // keep polling
        continue;
      }
    } // done polling sync bit

    // go through every node in the partition    
    bool made_an_update = true;
    for( uint64_t node = start_node; node <= end_node; node++ ) {
      // make string out of node number
      string u = boost::lexical_cast<std::string>(node);

      // go read out the node's edge list and distance
      try {
        client.read(graph_tableid, u.c_str(), u.length(), &rc_read_buf);
      } 
      // if the node doesn't exist, then skip over it
      // note: can we use RejectRules to know for sure that the node doesn't exist?
      catch (RAMCloud::ClientException& e) {
        continue;
      }

      uint32_t length = rc_read_buf.getTotalLength();
      string u_val(static_cast<const char*>(rc_read_buf.getRange(0, length)), length);
      std::vector<string> u_edge_list;
      boost::split(u_edge_list, u_val, boost::is_any_of(" "));
      int64_t dist_to_u = boost::lexical_cast<int>(u_edge_list.back());
      u_edge_list.pop_back();
      
      // by now we have:
      //  - u (string): node we're visiting
      //  - u_edge_list (vector<string>): array of neighbors to u
      //  - dist_to_u (int64_t): distance from source node to u
     
      // go visit all of the neighbors, updating distances if necessary
      if( dist_to_u != -1 && !u_edge_list.empty()) {
        for( std::vector<string>::iterator it = u_edge_list.begin(); it != u_edge_list.end(); ++it ) {
          uint64_t version;
          string v = *it;

          // for each neighbor, scope him out and update if necessary
          while(1) {
            // first see if the neighbor exists in ramcloud
            try {
              client.read(graph_tableid, v.c_str(), v.length(), &rc_read_buf, NULL, &version);
            } 
            // if the node doesn't exist, then write a new entry for him
            catch (RAMCloud::ClientException& e) {
              // but need to check a race condition:
              // multiple compute slaves may be racing to create the same node
              RejectRules rules;
              memset(&rules, 0, sizeof(rules));
              rules.exists = 1;
              try {
                client.write(graph_tableid, v.c_str(), v.length(), (boost::lexical_cast<string>(dist_to_u+1)).c_str(), &rules);
              } catch (RAMCloud::RejectRulesException& e) {
                // another compute slave beat us in the race to create this node
                // and unfortunately we don't know the new distance. Since we may
                // have a shorter distance to this neighbor, we need to restart
                // the loop and scope out the neighbor's distance value.
                continue;
              }
              made_an_update = true;
              // std::cout << boost::lexical_cast<string>(id) + ": " + u + ": created " + v + " with value " + boost::lexical_cast<string>(dist_to_u+1) + "\n";
              // we won the race and created the neighbor, so we're done
              // continue iterating through the edges for u
              break;
            }

            // the node exists in ramcloud, lets observe its current distance value
            length = rc_read_buf.getTotalLength();
            string v_val(static_cast<const char*>(rc_read_buf.getRange(0, length)), length);
            std::vector<string> v_edge_list;
            boost::split(v_edge_list, v_val, boost::is_any_of(" "));
            int64_t dist_to_v = boost::lexical_cast<int>(v_edge_list.back());

            // check to see if our distance to v beats its current distance value
            if( dist_to_v == -1 || dist_to_u + 1 < dist_to_v ) {
              // we've got a shorter distance to v, so we should update v
              // but need to check a race condition:
              // multiple compute slaves may be racing to update the same node
              RejectRules rules;
              memset(&rules, 0, sizeof(rules));
              rules.givenVersion = version;
              rules.versionNeGiven = 1;
              string new_v_val = v_val.substr(0, v_val.length() - v_edge_list.back().length()).append(boost::lexical_cast<string>(dist_to_u + 1));
              
              try {
                client.write(graph_tableid, v.c_str(), v.length(), new_v_val.c_str(), &rules);
              } catch (RAMCloud::ClientException& e) {
                // another compute slave beat us in the race to update this node
                // and unfortunately we don't know the new distance. It may still 
                // be the case that we have a shorter distance to offer, so we need
                // to restart the loop and try this again.
                continue;
              }
              made_an_update = true;
              // std::cout << boost::lexical_cast<string>(id) + ": " + u + ": updated " + v + " to " + boost::lexical_cast<string>(dist_to_u+1) + "\n";
 
              // we won the race and updated the neighbor, so we're done
              // continue iterating through the edges for u
              break;
            } else {
              // we don't have a better distance value to offer
              // continue on to the next edge
              break;
            }
          } // while loop for scoping out a particular neighbor
        } // neighbor iteration for loop
      } else {
        // our distance is Inf, or we have no neighbors
        // so do nothing in this case, move on to next node in our partition
      }
    } // node iteration for loop

    // we've finished iterating through all our nodes
    // if we've made an update to any node in this superstep, then we 
    // vote not to be finished
    while(1) {
      uint64_t version;
      try {
        client.read(graph_tableid, DONE_VEC_KEY, sizeof(DONE_VEC_KEY)-1, &rc_read_buf, NULL, &version);
      } catch (RAMCloud::ClientException& e) {
        // for some reason the done vector is not there?
        // just keep polling, lock-up can be signal
        continue;
      }
      
      uint32_t length = rc_read_buf.getTotalLength();
      string val(static_cast<const char*>(rc_read_buf.getRange(0, length)), length); 

      if( made_an_update ) {
        val.replace(id, 1, "0");
      } else {
        val.replace(id, 1, "1");
      }

      RejectRules rules;
      memset(&rules, 0, sizeof(rules));
      rules.givenVersion = version;
      rules.versionNeGiven = 1;
  
      try {
        client.write(graph_tableid, DONE_VEC_KEY, sizeof(DONE_VEC_KEY)-1, val.c_str(), &rules);
      } catch (RAMCloud::ClientException& e) {
        // we lost the write race, try again
        continue;        
      }
      
      // we won the write race
      break;
    } // write the done vector

    // set the sync bit back to one
    while(1) {
      uint64_t version;
      try {
        client.read(graph_tableid, SYNC_VEC_KEY, sizeof(SYNC_VEC_KEY)-1, &rc_read_buf, NULL, &version);
      } catch (RAMCloud::ClientException& e) {
        continue;
      }

      uint32_t length = rc_read_buf.getTotalLength();
      string val(static_cast<const char*>(rc_read_buf.getRange(0, length)), length);  
      val.replace(id, 1, "0");

      RejectRules rules;
      memset(&rules, 0, sizeof(rules));
      rules.givenVersion = version;
      rules.versionNeGiven = 1;
 
      try {
        client.write(graph_tableid, SYNC_VEC_KEY, sizeof(SYNC_VEC_KEY)-1, val.c_str(), &rules);
      } catch (RAMCloud::ClientException& e) {
        continue;
      }

      break;
    } // wrie the sync vector
  } // program while loop

  return 0;
} // end of program
