#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/random.hpp>
#include <boost/generator_iterator.hpp>
#include <iostream>
#include <fstream>
#include <set>

int main(int argc, char* argv[]) {

  /* Number of desired nodes. */
  uint64_t num_nodes = boost::lexical_cast<uint64_t>(argv[1]);
  /* Desired average node out degree. */
  uint64_t out_deg = boost::lexical_cast<uint64_t>(argv[2]);
  /* Output file for adjacency list. */
  std::fstream adj_list_file(argv[3], std::fstream::out);

  if(out_deg >= num_nodes) {
    std::cout << "ERROR: Desired average node out degree is too large\n";
    return 0;
  }

  std::set<uint64_t> neighbor_set;
  uint64_t neighbor;

  boost::mt19937 rng;
  boost::binomial_distribution<uint64_t> binom_dist(num_nodes-1, (double)out_deg/(double)(num_nodes-1));
  boost::uniform_int<uint64_t> unif_dist(0,num_nodes-1);
  boost::variate_generator<boost::mt19937, boost::binomial_distribution<uint64_t>> binom_dice(rng, binom_dist);
  boost::variate_generator<boost::mt19937, boost::uniform_int<uint64_t>> unif_dice(rng, unif_dist);

  uint64_t stat_tot_edges = 0;

  for(uint64_t node = 0; node<num_nodes; node++) {
    adj_list_file << node;

    uint64_t num_edges = binom_dice();
    stat_tot_edges += num_edges;

    neighbor_set.insert(node);

    for(uint64_t edge = 0; edge<num_edges; edge++) {
      while(neighbor_set.count(neighbor = unif_dice()) !=0 ) {}
      neighbor_set.insert(neighbor);
    }

    neighbor_set.erase(node);

    for(std::set<uint64_t>::iterator it = neighbor_set.begin(); it != neighbor_set.end(); ++it) {
      adj_list_file << " " << *it;
    }

    neighbor_set.clear();

    adj_list_file << "\n";
  }

  std::cout << "Graph generation complete" << "\n";
  std::cout << "Stats:" << "\n";
  std::cout << "\tTotal Nodes: " << num_nodes << "\n";
  std::cout << "\tTotal Edges: " << stat_tot_edges << "\n";
  std::cout << "\tAvg Out Deg: " << (float)stat_tot_edges/(float)num_nodes << "\n";

  return 0;
}
