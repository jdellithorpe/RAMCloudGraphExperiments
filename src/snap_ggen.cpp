#include "Snap.h"
#include <string>

int main(int argc, char* argv[]) {
  const uint64_t NNodes = atoi(argv[1]);
  const uint64_t NEdges = atoi(argv[2]);

  PNGraph NGraph;

  // Generate directed graph
  NGraph = TSnap::GenRndGnm<PNGraph>(NNodes, NEdges, true, TInt::Rnd);

  // Write graph to file
  TSnap::SaveEdgeList(NGraph, ("genrndgnm." + std::string(argv[1]) + "n_" + std::string(argv[2]) + "m.edge_list").c_str(), "Yeehaw!");

  return 0;
}
