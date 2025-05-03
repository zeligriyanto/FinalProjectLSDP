Reif Birzin, Olivia McCarty, Zelig Riyanto
# Graph Matching Project Submission

Below are the results from running three graph matching algorithms on various datasets, followed by descriptions of each algorithm and a strategy for selecting the appropriate method on new graphs.

The output files, even when zipped, were too large to submit through GitHub, so I have shared them through a google drive link: https://drive.google.com/drive/folders/1bVEUgknWs382CJdQYcuhBjaIPKYH-2Y9?usp=drive_link. Note that each solution file is the algorithm that gave the most amount of matches for each graph.

## Results

| File                    | Edges        | Algorithm | Method | Matches Found | Time to Run (s) | Rounds (Only applicable to Luby’s) |
|-------------------------|--------------|-----------|--------|---------------|-----------------|-------------------------------------|
| com-okurt.ungraph       | 117,185,083  | Luby      | GCP    | 1,336,831     | 1128.47         | 10                                  |
| twitter_original_edges  | 63,555,749   | Luby      | GCP    | 92,214        | 419.90          | 8                                   |
| soc-LiveJournal1        | 42,851,237   | Luby      | GCP    | 1,571,713     | 787.96          | 10                                  |
| soc-pokec-relationships | 22,301,964   | Luby      | GCP    | 595,982       | 279.57          | 9                                   |
| soc-pokec-relationships | 22,301,964   | Greedy    | GCP    | 739,254       | 211.741         | —                                   |
| musae_ENGB_edges        | 35,324       | Luby      | GCP    | 2,288         | 34.64           | 7                                   |
| musae_ENGB_edges        | 35,324       | Greedy    | Local  | 2,750         | 13.12           | —                                   |
| log_normal_100          | 2,671        | Luby      | GCP    | 50            | 29.45           | 7                                   |
| log_normal_100          | 2,671        | Greedy    | GCP    | 49            | 11.73           | —                                   |
| log_normal_100          | 2,671        | Edmonds’  | GCP    | 50            | 0.10            | —                                   |

*All programs ran on GCP were done with 3×4 N1 core CPU.*

## Edmonds’ Blossom Algorithm

The idea behind this algorithm is to repeatedly find augmenting paths, when an odd cycle, or a “blossom” is hit, contract it and continue searching. The benefit of using this algorithm is that it proven that it finds the maximum matching, meaning that it guarantees the largest possible number of matched edges. The proof is provided in this article: https://stanford.edu/~rezab/classes/cme323/S16/projects_reports/shoemaker_vare.pdf

However, this algorithm only worked successfully on the smallest of the provided graphs. Because it is very complex and memory intensive, the program froze when running on the musae_ENGB_edges graph after finding 2183 matches. This prevented us from attempting to run the algorithm on any of the larger graphs. For small graphs of up to a few thousand edges, this is the idea algorithm as it gives the maximum matching.

## Greedy Algorithm

The idea behind this algorithm is to prioritize edges with fewer connections, matching “rare” nodes before they are taken. This approach is fast and simple, as can be seen comparing the runtime of this compared to the Luby-inspired algorithm on the musae_ENGB_edges graph. Additionally, this algorithm avoids oversaturating high-degree nodes early due to its focus on “rare” nodes. While this algorithm does provide a large percentage of the maximum possible matches, the result is dependent on the order that edges are read in. This method can also be memory intensive, leading to poor performance on large, dense graphs. The greedy algorithm is ideally used on medium sized graphs with tens of thousands of edges, providing a more optimal matching than the luby-inspired algorithm while simultaneously being faster.

## Luby-Inspired Algorithm

This algorithm is approached in rounds, during which edges are assigned random priorities and each vertex selects its lowest-priority neighbor. An edge is added to the matching only if both endpoints choose it, and the process repeats until convergence. The primary benefit of this algorithm is that it scales very well on massive graphs with millions of edges because it does not require pre-sorting or global degree computation. However, this approach does not always give the largest matching as seen by comparing the size of the matchings of the musae_ENGB_edges graph. The number of rounds completed depends on the density and size of the graph, taking up to 10 rounds to compute the matching of the largest graph. While the runtime is not unreasonable, it can add up, taking nearly 20 minutes to run the program on the largest graph. Additionally, results are dependent on the random priorities, so each time running the algorithm may give a slightly different size matching. This luby-inspired algorithm is ideally used on very large graphs since it is able to successfully run on graphs with over 100 million edges.

## Strategy for New Test Case

As mentioned in the descriptions of each algorithm, choosing which to run on a new graph would be primarily dependent on the number of edges. On smaller graphs of a few thousand edges, Edmonds’ Blossom Algorithm would successfully output the maximum matching. For slightly larger graphs, the Greedy Algorithm would provide a mostly optimal matching while maintaining a reasonable runtime. Lastly, for the largest graphs of over 10 million edges, the Luby-Inspired Algorithm would successfully output a matching with very reasonable size.

