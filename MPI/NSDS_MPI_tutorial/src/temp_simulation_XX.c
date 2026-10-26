#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

// Group number:
// Group members:

const double L = 100.0;                 // Length of the 1d domain
const int n = 1000;                     // Total number of points
const int iterations_per_round = 1000;  // Number of iterations for each round of simulation
const double allowed_diff = 0.001;      // Stopping condition: maximum allowed difference between values

double initial_condition(double x, double L) {
    return fabs(x - L / 2);
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    double* temperatures = (double*) malloc(sizeof(double) * n / size);
    double* new_values = (double*) malloc(sizeof(double) * n / size);

    for(int i=0; i<n/size; i++){
	temperatures[i] = initial_condition((n*rank/size + i) * L/(n-1), L);
	new_values[i] = 0.0;      // Perform one round of iterations
    }

    int round = 0;
    while(1){
        round++;
	double round_min = temperatures[n/size-1], round_max = temperatures[0];
        for (int t = 0; t < iterations_per_round; t++) {
	    double first_temp, last_temp;
	    // printf("%d : reached iteration %d : before send/recv blocks\n", rank, t);
            if(rank < size-1){
		MPI_Send(&temperatures[n/size - 1], 1, MPI_DOUBLE, rank+1, 0, MPI_COMM_WORLD);
		MPI_Recv(&last_temp, 1, MPI_DOUBLE, rank+1, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	    }
	    if(rank > 0){
		MPI_Send(&temperatures[0], 1, MPI_DOUBLE, rank-1, 0, MPI_COMM_WORLD);
		MPI_Recv(&first_temp, 1, MPI_DOUBLE, rank-1, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	    }
	    
	    new_values[0] = rank>0 ? (first_temp + temperatures[0] + temperatures[1])/3 : (temperatures[0]+temperatures[1])/2;
	    new_values[n/size-1] = rank<size-1 ? (temperatures[n/size-2] + temperatures[n/size-1] + last_temp)/3 : (temperatures[n/size-2]+temperatures[n/size-1])/2;
	    for(int i=1; i<n/size-1; i++){
		new_values[i] = (temperatures[i-1] + temperatures[i] + temperatures[i+1]) / 3;
	    }
	    for(int i=0; i<n/size; i++){
		temperatures[i] = new_values[i];
		if(temperatures[i] < round_min) round_min = temperatures[i];
		if(temperatures[i] > round_max) round_max = temperatures[i];
	    }
        }

	// MPI_Barrier(MPI_COMM_WORLD);
	// printf("%d : reached end of round %d\n", rank, round);
        double global_min, global_max, max_diff;
	MPI_Allreduce(&round_min, &global_min, 1, MPI_DOUBLE, MPI_MIN, MPI_COMM_WORLD);
	MPI_Allreduce(&round_max, &global_max, 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
	// printf("%d : done reduce\n", rank);
	max_diff = global_max - global_min;
        if (rank == 0) {
	    printf("Round: %d\tMin: %f.5\tMax: %f.5\tDiff: %f.5\n", round, global_min, global_max, max_diff);
        } 
        if(max_diff < allowed_diff) break;
    }

    free(temperatures);
    free(new_values);

    MPI_Finalize();
    return 0;
}
