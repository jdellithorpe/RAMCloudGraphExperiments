#include "Snap.h"
#include <string>

int main(int argc, char* argv[]) {
  const TStr input_file(argv[1]);

  PNGraph NGraph;

  NGraph = TSnap::LoadConnList<PNGraph>(input_file);

  TIntH dist;
  TSnap::GetShortPath<PNGraph>(NGraph, 0, dist, true);

  TIntH::TIter dist_iter;

  for (dist_iter = dist.BegI(); dist_iter < dist.EndI(); dist_iter++) {
    printf("%d %d\n", (int)dist_iter.GetKey(), (int)dist_iter.GetDat());
  }

  return 0;
}
