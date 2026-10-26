#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <mpi.h>

// Creates an array of random numbers.
int *create_random_array(int num_elements, int max_value) {
  int *arr = (int *) malloc(sizeof(int)*num_elements);
  for (int i=0; i<num_elements; i++) {
    arr[i] = (rand() % max_value);
  }
  return arr;
}

// Process 0 selects a number num.
// All other processes have an array that they filter to only keep the elements
// that are multiples of num.
// Process 0 collects the filtered arrays and print them.
int main(int argc, char** argv) {
  // Maximum value for each element in the arrays
  const int max_val = 100;
  // Number of elements for each processor
  int num_elements_per_proc = 50;
  // Number to filter by
  int num_to_filter_by = 2;
  if (argc > 1) {
    num_elements_per_proc = atoi(argv[1]);
  }

  MPI_Init(NULL, NULL);

  int my_rank, world_size; 
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  // Init random number generator
  srand(time(NULL) * (my_rank + 1));

  // Process 0 selects the num
  int num;
  if (my_rank == 0) {
    num = num_to_filter_by;
  }

  MPI_Bcast(&num, 1, MPI_INT, 0, MPI_COMM_WORLD);
  printf("%d : sent bcast\n", my_rank);
  if(my_rank != 0){
    printf("%d : received num\n", my_rank);
    int* numbers = create_random_array(num_elements_per_proc, max_val);
    int multiples = 0;
    for(int i=0; i<num_elements_per_proc; i++){
      if(numbers[i] % num == 0){
        multiples++;
      }
    }
    int* multiples_values = (int*) malloc(sizeof(int) * multiples);
    int j = 0;
    for(int i=0; i<num_elements_per_proc; i++){
      if(numbers[i] % num == 0){
        multiples_values[j] = numbers[i];
        j++;
      }
    }
    MPI_Send(multiples_values, multiples, MPI_INT, 0, 0, MPI_COMM_WORLD);
    printf("%d : sent multiples\n", my_rank);

    free(numbers);
    free(multiples_values);
  } else {
    int size = 0;
    int** arrays = (int**) malloc(sizeof(int*) * (world_size-1));
    int num_msgs[3];
    int first_free_id = 0;
    for(int i=1; i<world_size; i++){
      MPI_Status status;
      MPI_Probe(i, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      printf("%d : done probe\n", my_rank);
      MPI_Get_count(&status, MPI_INT, num_msgs+i-1);
      arrays[i-1] = (int*) malloc(sizeof(int) * num_msgs[i-1]);
      MPI_Recv(arrays[i-1], num_msgs[i-1], MPI_INT, i, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      printf("%d : received a multiple\n", my_rank);
      size += num_msgs[i-1];
    }
    int* final_array = (int*) malloc(sizeof(int) * size);
    int c = 0;
    for(int i=0; i<world_size-1; i++){
      for(int j=0; j<num_msgs[i]; j++){
        final_array[c] = arrays[i][j];
        c++;
      }
    }

    for(int h=0; h<size; h++){
      printf("%d ", final_array[h]);
    }

    for(int i=0; i<world_size-1; i++){
      free(arrays[i]);
    }
    free(arrays);
    free(final_array);
  }

  
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
}
