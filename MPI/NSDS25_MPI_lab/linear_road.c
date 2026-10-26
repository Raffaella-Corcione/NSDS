#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/**
 * Group number:
 *
 * Group members
 * Member 1
 * Member 2
 * Member 3
 *
 **/

// Set DEBUG 1 if you want car movement to be deterministic
#define DEBUG 1

const int num_segments = 256;

const int num_iterations = 1000;
const int count_every = 10;

const double alpha = 0.5;
const int max_in_per_sec = 10;

// Returns the number of car that enter the first segment at a given iteration.
int create_random_input() {
#if DEBUG
  return 1;
#else
  return rand() % max_in_per_sec;
#endif
}

// Returns 1 if a car needs to move to the next segment at a given iteration, 0 otherwise.
int move_next_segment() {
#if DEBUG
  return 1;
#else
  return rand() < alpha ? 1 : 0;
#endif
}

int main(int argc, char** argv) { 
  MPI_Init(NULL, NULL);

  int rank;
  int num_procs;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
  srand(time(NULL) + rank);
  
  // each process takes care of a segment
  int* road = (int*) calloc(sizeof(int), num_segments/num_procs);
  int* moving = (int*) malloc(sizeof(int) *  num_segments/num_procs);  

  // Simulate for num_iterations iterations
  for (int it = 0; it < num_iterations; ++it) {
    // Move cars across segments
    int new_cars = 0;
    for(int i=0; i<num_segments/num_procs; i++){
      moving[i] = move_next_segment() * road[i];
    }
    // New cars may enter in the first segment
    if(rank == 0){
	new_cars = create_random_input();
	road[0] += new_cars;
    }
    // Cars may exit from the last segment

    else {
        MPI_Recv(&new_cars, 1, MPI_INT, rank-1, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE); 
	 road[0] += new_cars;
    }
    road[0] -= moving[0];
    for(int i=1; i<num_segments/num_procs; i++){
	road[i] += moving[i-1];
	road[i] -= moving[i];	
    }
    if(rank != num_procs - 1){
	MPI_Send(&moving[num_segments/num_procs-1], 1, MPI_INT, rank+1, 0, MPI_COMM_WORLD);
    }

    // When needed, compute the overall sum
    if (it%count_every == 0) {
      int global_sum = 0, local_sum = 0;
      for(int i=0; i<num_segments/num_procs; i++){
	local_sum += road[i];
      }
      
      MPI_Reduce(&local_sum, &global_sum, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
      
      if (rank == 0) {
	printf("Iteration: %d, sum: %d\n", it, global_sum);
      }
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
  }

  free(road); 
  free(moving);
  
  MPI_Finalize();
}
