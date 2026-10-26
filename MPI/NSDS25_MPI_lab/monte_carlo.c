#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <mpi.h>

const int num_iter_per_proc = 10 * 1000 * 1000;

int main() {
  MPI_Init(NULL, NULL);
    
  int rank;
  int num_procs;
  int sum = 0;
  
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

  srand(time(NULL) + rank);

  for(int i=0; i<num_iter_per_proc; i++){
    float x, y;
    x = (float)rand()/(float)RAND_MAX; y = (float)rand()/(float)RAND_MAX;
    if(x*x + y*y <= 1) sum++;
  }

  if(rank != 0){
    MPI_Send(&sum, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
  }
  
  if (rank == 0) {
    for(int i=0; i<num_procs-1; i++){
      MPI_Status status;
      MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      int num_msgs;
      MPI_Get_count(&status, MPI_INT, &num_msgs);
      int source = status.MPI_SOURCE;
      int rec;
      MPI_Recv(&rec, num_msgs, MPI_INT, source, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      sum += rec;
    }

    double pi = (4.0*sum) / (num_iter_per_proc*num_procs);
    printf("Pi = %f\n", pi);
  }
    
  MPI_Finalize();
  return 0;
}
